"""
End-to-end test for the Blast Type / AI Reply Context template flow.

Covers the happy-path templates AND the hardening work:
  - tenant isolation (two tenants with simultaneous context — neither leaks)
  - session is retrievable by the real inbound helper
  - the AI prompt wrap fires for both the Zarna (performer) path and the SMB
    brain path
  - unscoped callers still function but are flagged in logs

Run:
    export DATABASE_URL=...   # same URL the operator backend uses
    python scripts/test_blast_mode_e2e.py

Cleans up every row it writes.
"""
from __future__ import annotations

import io
import logging
import os
import sys
import textwrap

import psycopg2

# Bring the ZarnaAi app package on the path so we can import the real
# blast_context helpers that inbound messages hit.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.live_shows.blast_context import (  # noqa: E402
    get_active_blast_context,
    build_blast_context_prompt,
)


# ────────────────────────────────────────────────────────────────────────
# These MUST stay in lockstep with lovable-frontend/src/components/blast/types.ts
# (buildBlastModeTemplate). If you change one, change the other.
# ────────────────────────────────────────────────────────────────────────
def build_mode_template(mode: str, *, answer="", prize="", deadline="", topic="") -> str:
    if mode == "pop_quiz":
        answer = (answer or "").strip() or "[WRITE THE CORRECT ANSWER HERE]"
        return (
            f"This blast is a pop quiz. The correct answer is: {answer}.\n\n"
            f"When fans reply, tell them if they got it right — be generous with partial "
            f"answers and phonetically close guesses (e.g. \"Purple Rayne\" counts as "
            f"\"Purple Rain\"). Celebrate correct answers with hype and stay in character. "
            f"Playfully roast wrong answers but drop the real answer so they still learn it. "
            f"1–2 sentences max, no lists."
        )
    if mode == "contest":
        prize = (prize or "").strip() or "[PRIZE]"
        deadline = (deadline or "").strip() or "[WHEN WINNER IS DRAWN]"
        return (
            f"This blast is a contest giveaway. Prize: {prize}. Winner drawn: {deadline}.\n\n"
            f"Fans who reply are entering. Confirm their entry warmly in 1 sentence, "
            f"staying in character. If they ask how to enter or when results come out, "
            f"answer using the prize/deadline above. If they ask about anything else, "
            f"answer normally but nudge them that they're already entered."
        )
    if mode == "qa":
        topic = (topic or "").strip() or "[TOPIC]"
        return (
            f"Fans are replying with questions about {topic}.\n\n"
            f"Answer in character — warm, funny, direct. 2 sentences max per reply. "
            f"If you don't know the answer, tell them honestly and say you'll get back to them."
        )
    return ""


TEST_TAG = "e2e-blast-mode-test"
TEST_EMAIL = f"{TEST_TAG}@example.com"


def _conn():
    url = os.environ["DATABASE_URL"].replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def _save_draft_raw(*, body: str, context_note: str, creator_slug: str) -> int:
    """Mirror operator/app/queries.save_blast_draft INSERT path."""
    conn = _conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blast_drafts
                  (name, body, channel, audience_type, audience_filter,
                   audience_sample_pct, media_url, link_url, tracked_link_slug,
                   is_quiz, quiz_correct_answer, blast_context_note, status,
                   created_by, creator_slug)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s,%s) RETURNING id
                """,
                (
                    f"E2E {TEST_TAG} draft",
                    body,
                    "twilio",
                    "all",
                    "",
                    100,
                    "",
                    "",
                    "",
                    False,
                    "",
                    context_note,
                    TEST_EMAIL,
                    creator_slug,
                ),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _reload_draft_scoped(draft_id: int, creator_slug: str) -> dict | None:
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id,
                       COALESCE(blast_context_note, '') AS blast_context_note,
                       creator_slug, body, created_by
                FROM   blast_drafts
                WHERE  id = %s AND creator_slug = %s
                """,
                (draft_id, creator_slug),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "blast_context_note": row[1],
                "creator_slug": row[2],
                "body": row[3],
                "created_by": row[4],
            }
    finally:
        conn.close()


def _simulate_send_context_session(
    draft_id: int, body: str, context_note: str, creator_slug: str
) -> None:
    """Mirror operator.app.blast_sender._create_blast_context_session exactly,
    including the pre-send ordering and creator_slug stamping.
    """
    combined = f"The blast message that was sent: \"{body}\""
    if context_note.strip():
        combined += f"\n\nAdditional context from the operator: {context_note.strip()}"
    conn = _conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO blast_context_sessions
                  (blast_draft_id, context_note, creator_slug, expires_at)
                VALUES (%s, %s, %s, NOW() + INTERVAL '24 hours')
                """,
                (draft_id, combined, creator_slug),
            )
    finally:
        conn.close()


def _cleanup(draft_ids, extra_slugs=()):
    if not draft_ids and not extra_slugs:
        return
    conn = _conn()
    try:
        with conn, conn.cursor() as cur:
            if draft_ids:
                cur.execute(
                    "DELETE FROM blast_context_sessions WHERE blast_draft_id = ANY(%s)",
                    (list(draft_ids),),
                )
                cur.execute("DELETE FROM blast_drafts WHERE id = ANY(%s)", (list(draft_ids),))
            if extra_slugs:
                cur.execute(
                    "DELETE FROM blast_context_sessions WHERE creator_slug = ANY(%s)",
                    (list(extra_slugs),),
                )
    finally:
        conn.close()


def _check(label: str, cond: bool, extra: str = "") -> bool:
    status = "PASS" if cond else "FAIL"
    marker = "[" + status + "]"
    suffix = f" — {extra}" if extra else ""
    print(f"  {marker}  {label}{suffix}")
    return cond


def run() -> int:
    print("Blast Type / Context Template — end-to-end test")
    print("=" * 70)

    zarna_slug = "zarna"
    other_slug = "e2e_other_tenant_slug"   # fake tenant for isolation test
    created: list[int] = []
    passed = True

    try:
        for mode, fields, expected_fragments in (
            (
                "pop_quiz",
                {"answer": "Purple Rain"},
                [
                    "This blast is a pop quiz",
                    "correct answer is: Purple Rain",
                    "phonetically close guesses",
                ],
            ),
            (
                "contest",
                {"prize": "2 tickets to the NYC show", "deadline": "Friday 11:59pm ET"},
                [
                    "This blast is a contest giveaway",
                    "Prize: 2 tickets to the NYC show",
                    "Winner drawn: Friday 11:59pm ET",
                ],
            ),
            (
                "qa",
                {"topic": "my Netflix special dropping Friday"},
                [
                    "Fans are replying with questions about my Netflix special",
                    "2 sentences max",
                ],
            ),
        ):
            print(f"\n→ Mode: {mode}")
            template = build_mode_template(mode, **fields)

            passed &= _check(
                f"{mode}: template contains expected fragments",
                all(frag in template for frag in expected_fragments),
            )

            body = f"E2E test body for {mode} — {TEST_TAG}"
            draft_id = _save_draft_raw(
                body=body, context_note=template, creator_slug=zarna_slug
            )
            created.append(draft_id)
            passed &= _check(f"{mode}: draft persisted (id={draft_id})", bool(draft_id))

            reloaded = _reload_draft_scoped(draft_id, zarna_slug)
            passed &= _check(f"{mode}: tenant-scoped reload returns row", reloaded is not None)
            if reloaded:
                passed &= _check(
                    f"{mode}: blast_context_note persisted verbatim",
                    reloaded.get("blast_context_note") == template,
                )

            # Create the context session BEFORE send (matches new ordering).
            _simulate_send_context_session(draft_id, body, template, zarna_slug)

            # ── Scoped read: zarna fan sees zarna context ──────────────
            active = get_active_blast_context(creator_slug=zarna_slug)
            passed &= _check(
                f"{mode}: scoped read returns active session",
                bool(active),
            )
            passed &= _check(
                f"{mode}: scoped read carries template content",
                active is not None and all(frag in active for frag in expected_fragments),
            )

            # ── Tenant isolation: OTHER tenant must NOT see zarna's context ──
            other_active = get_active_blast_context(creator_slug=other_slug)
            passed &= _check(
                f"{mode}: OTHER tenant cannot see zarna's context (isolation)",
                other_active is None,
            )

            # ── Prompt wrap ────────────────────────────────────────────
            prompt_block = build_blast_context_prompt(active or "")
            passed &= _check(
                f"{mode}: prompt block has HIGH PRIORITY header",
                "BLAST CONTEXT — HIGH PRIORITY" in prompt_block,
            )
            passed &= _check(
                f"{mode}: prompt block carries template fragments",
                all(frag in prompt_block for frag in expected_fragments),
            )
            print("  --- rendered AI prompt block (truncated) ---")
            preview = "\n".join(prompt_block.splitlines()[:6])
            print(textwrap.indent(preview + "\n    …", "    "))

            conn = _conn()
            try:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM blast_context_sessions WHERE blast_draft_id = %s",
                        (draft_id,),
                    )
            finally:
                conn.close()

        # ── Cross-tenant race: BOTH slugs have active context simultaneously
        # (newer row is OTHER). Each tenant should still see ONLY their own.
        print("\n→ Cross-tenant race check")
        body_a = f"Cross-race body A — {TEST_TAG}"
        body_b = f"Cross-race body B — {TEST_TAG}"
        draft_a = _save_draft_raw(body=body_a, context_note="A context", creator_slug=zarna_slug)
        draft_b = _save_draft_raw(body=body_b, context_note="B context", creator_slug=other_slug)
        created.extend([draft_a, draft_b])

        _simulate_send_context_session(draft_a, body_a, "A context", zarna_slug)
        # B is newer (inserted second) — globally-most-recent under the old
        # unscoped query would return B for zarna fans. Under scoped queries,
        # zarna still gets A.
        _simulate_send_context_session(draft_b, body_b, "B context", other_slug)

        z = get_active_blast_context(creator_slug=zarna_slug) or ""
        o = get_active_blast_context(creator_slug=other_slug) or ""
        passed &= _check(
            "race: zarna sees only zarna's row",
            "A context" in z and "B context" not in z,
            f"z={z[:80]!r}",
        )
        passed &= _check(
            "race: other tenant sees only its own row",
            "B context" in o and "A context" not in o,
            f"o={o[:80]!r}",
        )

        # ── Unscoped legacy path still works but emits WARNING ──────────
        print("\n→ Legacy unscoped path")
        warn_buf = io.StringIO()
        handler = logging.StreamHandler(warn_buf)
        handler.setLevel(logging.WARNING)
        logging.getLogger("app.live_shows.blast_context").addHandler(handler)
        try:
            legacy = get_active_blast_context()  # no slug
        finally:
            logging.getLogger("app.live_shows.blast_context").removeHandler(handler)
        passed &= _check(
            "legacy: unscoped call still returns a row",
            bool(legacy),
        )
        passed &= _check(
            "legacy: unscoped call logged a WARNING",
            "cross-tenant unsafe" in warn_buf.getvalue(),
        )

    finally:
        _cleanup(created, extra_slugs=[other_slug])
        print(f"\nCleanup: removed {len(created)} test draft(s) + tenant={other_slug!r} sessions.")

    print("\n" + "=" * 70)
    print("RESULT:", "ALL CHECKS PASSED" if passed else "SOME CHECKS FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(run())
