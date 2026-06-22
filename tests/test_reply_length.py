"""Regression tests for reply-length handling.

Covers the bug where a "give me 3 jokes" reply was cut off right after the
"1." enumerator: the old _trim_reply hard-capped output at 3 sentences and
counted the list marker "1." as a whole sentence, dropping the actual jokes.

Now:
  - _trim_reply no longer caps by sentence count (only a generous char ceiling).
  - Long replies are split into multiple SMS by split_for_sms instead of being
    truncated mid-thought.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.brain.generator as gen
from app.utils.sms_segments import split_for_sms


# ---------------------------------------------------------------------------
# _trim_reply: list-style replies survive intact
# ---------------------------------------------------------------------------

def test_trim_reply_keeps_numbered_list():
    """The exact failure mode: a 3-item joke list must not be cut after '1.'."""
    reply = (
        "Okay, for Sanjay's birthday, here are three jokes you can use. "
        "He won't even see them coming because he's probably asleep. "
        "1. Sanjay loves Claude so much he asked it to write his birthday card — "
        "it ghosted him. "
        "2. He can sleep anytime, anywhere — his superpower is the nap, his "
        "kryptonite is staying awake. "
        "3. He never stops yapping, so this is the one day we let him."
    )
    out = gen._trim_reply(reply)
    assert "1." in out and "2." in out and "3." in out
    assert out.rstrip().endswith("we let him.")
    # Must NOT end right after the first enumerator like the old bug did.
    assert not out.rstrip().endswith("1.")


def test_trim_reply_leaves_short_prose_unchanged():
    reply = "Hi back! What's going on — good day or a complaint?"
    assert gen._trim_reply(reply) == reply


def test_trim_reply_allows_more_than_three_sentences():
    reply = "One. Two. Three. Four. Five."
    out = gen._trim_reply(reply)
    assert "Four" in out and "Five" in out


def test_trim_reply_ceiling_trims_on_boundary_only_when_huge():
    huge = ("This is a sentence. " * 200).strip()  # ~4000 chars
    out = gen._trim_reply(huge)
    assert len(out) <= gen._MAX_CHARS
    # Trimmed on a clean boundary — never a dangling partial word.
    assert out.endswith(".") or out.endswith("…")


# ---------------------------------------------------------------------------
# split_for_sms
# ---------------------------------------------------------------------------

def test_split_short_message_single_part():
    assert split_for_sms("short and sweet", limit=400) == ["short and sweet"]


def test_split_empty():
    assert split_for_sms("", limit=400) == []
    assert split_for_sms("   ", limit=400) == []


def test_split_long_message_into_multiple_parts_preserving_content():
    body = (
        "Okay, here are three jokes you can use for Sanjay's birthday. "
        "1. He loves Claude so much that when he asked it for the meaning of life, "
        "it told him to take a nap, and he immediately agreed. "
        "2. Sanjay can fall asleep anywhere — give him a chair and a warm room and "
        "he is gone before the second sentence of any conversation. "
        "3. He never stops yapping, which is impressive for a man who is asleep "
        "seventy percent of the time, so happy birthday to a true multitasker."
    )
    parts = split_for_sms(body, limit=400)
    assert len(parts) >= 2
    for p in parts:
        assert len(p) <= 400
    # Every joke survives somewhere across the parts.
    joined = " ".join(parts)
    assert "1." in joined and "2." in joined and "3." in joined
    assert "multitasker" in joined


def test_split_respects_max_parts():
    body = "word " * 1000  # ~5000 chars
    parts = split_for_sms(body, limit=400, max_parts=4)
    assert len(parts) <= 4
    for p in parts:
        assert len(p) <= 400


def test_split_never_breaks_a_url_off_its_word():
    body = (
        "Get your tickets now before they sell out, this is going to be the show of "
        "the year and you do not want to miss a single minute of the chaos and the "
        "laughs and everything else, so go ahead and grab them right here today: "
        "https://zarnagarg.com/tickets/"
    )
    parts = split_for_sms(body, limit=160)
    # The URL must appear intact in exactly one part.
    assert sum("https://zarnagarg.com/tickets/" in p for p in parts) == 1


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"\u2713 {name}")
    print("All reply-length tests passed.")
