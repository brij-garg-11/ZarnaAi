"""
Verify that blast_context bypasses the hardcoded CLIP/SHOW/MERCH/BOOK/PODCAST
intent paths and falls through to GENERAL so the blast context controls the reply.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from app.brain.generator import _build_prompt
from app.brain.intent import Intent

BLAST_CTX = (
    "BLAST CONTEXT — HIGH PRIORITY. The fan just received a text about this topic "
    "and their reply is almost certainly related to it. Use this context to guide your response:\n"
    "Zarna just won the Webby Award for Best Short Form Video (People's Voice) with Malala. "
    "Watch it here: https://www.instagram.com/reels/DQCBdxbjHAL/\n"
    "Treat the fan's message as being about this topic.\n"
)

LINK_INTENTS = [Intent.CLIP, Intent.SHOW, Intent.MERCH, Intent.BOOK, Intent.PODCAST]


@pytest.mark.parametrize("intent", LINK_INTENTS)
def test_blast_context_bypasses_link_intent(intent):
    """With blast_context set, link-specific intents must fall through to GENERAL."""
    prompt = _build_prompt(
        intent=intent,
        user_message="What's the video?",
        chunks=[],
        history=[],
        blast_context=BLAST_CTX,
    )
    # The blast context string must appear in the final prompt
    assert "Webby Award" in prompt, f"Blast context missing from {intent} prompt"
    assert "instagram.com/reels" in prompt, f"Blast link missing from {intent} prompt"
    # None of the hardcoded link-intent markers should be present
    assert "zarnagarg.com/tickets" not in prompt, f"Ticket link leaked into {intent} prompt"
    assert "zarnagarg.com/merch" not in prompt, f"Merch link leaked into {intent} prompt"
    assert "zarnagarg.com/book" not in prompt, f"Book link leaked into {intent} prompt"


@pytest.mark.parametrize("intent", LINK_INTENTS)
def test_no_blast_context_uses_link_intent(intent):
    """Without blast_context, link-specific intents must use their own hardcoded path."""
    prompt = _build_prompt(
        intent=intent,
        user_message="What's the video?",
        chunks=[],
        history=[],
        blast_context=None,
    )
    # Blast context must NOT appear
    assert "BLAST CONTEXT" not in prompt, f"Blast context leaked into {intent} prompt without blast"
    # Each link intent should produce its own characteristic output
    link_markers = {
        Intent.CLIP:    "YouTube",
        Intent.SHOW:    "shows or tour dates",
        Intent.MERCH:   "merch",
        Intent.BOOK:    "book",
        Intent.PODCAST: "podcast episode",
    }
    assert link_markers[intent].lower() in prompt.lower(), (
        f"Expected {intent} path marker '{link_markers[intent]}' missing from prompt"
    )


def test_general_intent_always_includes_blast_context():
    """GENERAL intent includes blast context when provided."""
    prompt = _build_prompt(
        intent=Intent.GENERAL,
        user_message="Congrats!!",
        chunks=[],
        history=[],
        blast_context=BLAST_CTX,
    )
    assert "Webby Award" in prompt
    assert "instagram.com/reels" in prompt


def test_general_intent_without_blast_context_is_clean():
    """GENERAL intent without blast context produces no blast block."""
    prompt = _build_prompt(
        intent=Intent.GENERAL,
        user_message="Congrats!!",
        chunks=[],
        history=[],
        blast_context=None,
    )
    assert "BLAST CONTEXT" not in prompt
