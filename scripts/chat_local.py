"""
Local chat simulator — talk to any creator's bot without Twilio or SMS.

This calls the brain directly in-process (same code path that `/message`
runs for inbound SMS), so whatever you type and whatever it replies is
identical to what a real fan would experience on their phone.

Why this exists:
  - Twilio A2P approval is still pending, so real SMS is blocked.
  - The production `/message` endpoint uses a single global brain wired to
    Zarna — it can't talk to newly provisioned creators. This script
    builds a per-slug brain on the fly, so you can test HaleyBot /
    ThisMomCanComic / anyone right after provisioning.
  - Postgres storage is real: your conversation history, memory, and
    analytics rows persist across runs (great for exercising follow-up
    logic, session tracking, repeated-reply suppression, etc.).

Usage:
  python scripts/chat_local.py                       # Zarna, default phone
  python scripts/chat_local.py --slug haley          # HaleyBot
  python scripts/chat_local.py --slug zarna          # Zarna via the new slug-aware path
  python scripts/chat_local.py --slug haley --phone +15551234567

Slash-commands inside the REPL:
  /exit              quit
  /reset             wipe the fake fan's conversation + memory (fresh start)
  /new               pick a brand new fake phone number (simulates a new fan)
  /history [n]       dump the last N messages (default 20)
  /slug <new_slug>   hot-swap the brain to a different creator
  /phone             show the phone number in use

Tips:
  - For the new slug-aware path on Zarna, set PG_RETRIEVER_FOR_ZARNA=1
    before running this script. Otherwise Zarna uses EmbeddingRetriever
    (file-backed, production default).
  - Fake phone numbers start with +1555 so they can't collide with any
    real Twilio fan number if you later import into prod by mistake.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


# ---------------------------------------------------------------------------
# Logging — keep it quiet by default so the transcript is the main signal.
# Bump to DEBUG with CHAT_LOCAL_DEBUG=1 if you want to see retrieval / routing.
# ---------------------------------------------------------------------------

_debug = os.getenv("CHAT_LOCAL_DEBUG", "0").strip().lower() in ("1", "true", "yes")
logging.basicConfig(
    level=logging.DEBUG if _debug else logging.WARNING,
    format="%(levelname)s %(name)s: %(message)s",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_fake_phone() -> str:
    """Generate a +1555-prefixed fake phone that won't overlap real Twilio numbers."""
    return f"+1555{random.randint(100, 999):03d}{random.randint(1000, 9999):04d}"


def _build_brain(slug: Optional[str]):
    """Import-and-build — import inside the function so argparse --help doesn't
    pay the cost of loading Gemini clients / embeddings at module import."""
    from app.brain.handler import create_brain
    return create_brain(slug)


def _reset_fan(storage, phone: str) -> None:
    """
    Best-effort wipe of the fake fan's state. We only scrub data tied to
    this phone number; any creator-level rows (embeddings, configs) are
    untouched. Fails open — a missing table just means "nothing to clear"
    for that storage backend.
    """
    # InMemoryStorage uses plain dicts; just re-init those attrs we know about.
    for attr in ("messages", "memory", "tags", "location", "contacts"):
        data = getattr(storage, attr, None)
        if isinstance(data, dict):
            data.pop(phone, None)

    # PostgresStorage — scrub by phone across the tables we know about.
    # The storage class uses a ThreadedConnectionPool; _acquire/_release is
    # the public contract for getting a connection. Each DELETE runs in its
    # own savepoint so an unknown-table error on one table doesn't prevent
    # the rest from succeeding.
    if not (hasattr(storage, "_acquire") and hasattr(storage, "_release")):
        return
    try:
        conn = storage._acquire()  # type: ignore[attr-defined]
    except Exception as exc:
        print(f"(reset: could not acquire DB connection — {exc})")
        return
    try:
        for sql in (
            "DELETE FROM messages WHERE phone_number = %s",
            "DELETE FROM contacts WHERE phone_number = %s",
            "DELETE FROM fan_memory WHERE phone_number = %s",
            "DELETE FROM conversation_sessions WHERE phone_number = %s",
            "DELETE FROM fan_reply_context WHERE phone_number = %s",
        ):
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, (phone,))
                conn.commit()
            except Exception:
                # Likely a table that doesn't exist in this schema — roll
                # that failed transaction back and try the next DELETE.
                try:
                    conn.rollback()
                except Exception:
                    pass
    finally:
        try:
            storage._release(conn)  # type: ignore[attr-defined]
        except Exception:
            pass


def _print_history(storage, phone: str, n: int = 20) -> None:
    try:
        history = storage.get_conversation_history(phone, limit=n)
    except Exception as exc:
        print(f"(history unavailable: {exc})")
        return
    if not history:
        print("(no messages yet)")
        return
    for msg in history:
        who = "you" if msg.role == "user" else "bot"
        print(f"  [{who:>3}] {msg.text}")


# ---------------------------------------------------------------------------
# REPL
# ---------------------------------------------------------------------------

def _banner(slug: Optional[str], phone: str) -> None:
    print("┌" + "─" * 62 + "┐")
    print("│ " + f"Local Chat — slug={slug or '(legacy zarna)'}  phone={phone}".ljust(61) + "│")
    print("│ " + "type /help for commands, /exit to quit".ljust(61) + "│")
    print("└" + "─" * 62 + "┘")


def _help() -> None:
    print("  /exit                 quit")
    print("  /reset                wipe this fake fan's history + memory")
    print("  /new                  new random phone number (fresh fan)")
    print("  /history [n]          show last N messages (default 20)")
    print("  /slug <new_slug>      switch creator (rebuilds brain)")
    print("  /phone                show current phone number")
    print("  /debug on|off         toggle DEBUG logging for the next message")


def repl(slug: Optional[str], phone: str) -> int:
    print()
    print(f"[boot] building brain for slug={slug or '(None — legacy Zarna path)'} …")
    t0 = time.time()
    brain = _build_brain(slug)
    print(f"[boot] brain ready in {time.time() - t0:.1f}s  "
          f"(retriever={type(brain.retriever).__name__}, "
          f"storage={type(brain.storage).__name__})")
    _banner(slug, phone)

    # Track per-message latency so you can eyeball regressions against
    # Zarna's production target (~3-5s end-to-end).
    while True:
        try:
            line = input("you > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue

        # ── Slash commands ───────────────────────────────────────────────
        if line.startswith("/"):
            parts = line.split()
            cmd, args = parts[0].lower(), parts[1:]

            if cmd in ("/exit", "/quit", "/q"):
                return 0
            if cmd == "/help":
                _help()
                continue
            if cmd == "/reset":
                _reset_fan(brain.storage, phone)
                print(f"(reset: wiped history for {phone})")
                continue
            if cmd == "/new":
                phone = _fresh_fake_phone()
                print(f"(new fake fan: {phone})")
                continue
            if cmd == "/history":
                n = int(args[0]) if args and args[0].isdigit() else 20
                _print_history(brain.storage, phone, n)
                continue
            if cmd == "/slug":
                if not args:
                    print("(usage: /slug <new_slug>  — or /slug none to return to legacy Zarna)")
                    continue
                new_slug = None if args[0].lower() in ("none", "-", "") else args[0].strip().lower()
                print(f"[slug] rebuilding brain for slug={new_slug or '(None — legacy Zarna path)'} …")
                t0 = time.time()
                try:
                    brain = _build_brain(new_slug)
                except Exception as exc:
                    print(f"(slug switch failed: {exc})")
                    continue
                slug = new_slug
                print(f"[slug] brain ready in {time.time() - t0:.1f}s  "
                      f"(retriever={type(brain.retriever).__name__})")
                _banner(slug, phone)
                continue
            if cmd == "/phone":
                print(f"  phone = {phone}")
                continue
            if cmd == "/debug":
                want = (args[0].lower() if args else "on") in ("on", "1", "true", "yes")
                logging.getLogger().setLevel(logging.DEBUG if want else logging.WARNING)
                print(f"(debug {'ON' if want else 'OFF'})")
                continue

            print(f"(unknown command {cmd!r} — /help for options)")
            continue

        # ── Normal message → brain ───────────────────────────────────────
        t = time.time()
        try:
            reply = brain.handle_incoming_message(phone, line)
        except Exception as exc:
            print(f"(brain error: {exc})")
            continue
        dt = (time.time() - t) * 1000

        if not (reply or "").strip():
            print(f"bot > (no reply — conversation ender)  [{dt:.0f} ms]")
        else:
            print(f"bot > {reply}")
            print(f"      [{dt:.0f} ms]")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Interactive local chat with any creator's bot.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--slug", default=None,
        help="Creator slug (e.g. 'zarna', 'haley'). Omit for Zarna's legacy path.",
    )
    parser.add_argument(
        "--phone", default=None,
        help="Fake phone number to reuse across messages. Defaults to a fresh +1555… number.",
    )
    args = parser.parse_args()

    slug = (args.slug or "").strip().lower() or None
    phone = args.phone or _fresh_fake_phone()

    try:
        return repl(slug, phone)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(f"\nFatal: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
