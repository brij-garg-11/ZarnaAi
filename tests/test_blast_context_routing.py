"""
Verify blast_context routing:
- CLIP/MERCH/BOOK/PODCAST fall through to GENERAL so the blast context frames the reply.
- SHOW is the exception: an explicit tour/ticket question is always answered with the
  show path (+ directive), even while a blast context session is active, and the blast
  framing is NOT injected into it.
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
# SHOW intentionally NOT bypassed — an explicit tour question must still be answered.
BYPASS_INTENTS = [Intent.CLIP, Intent.MERCH, Intent.BOOK, Intent.PODCAST]


@pytest.mark.parametrize("intent", BYPASS_INTENTS)
def test_blast_context_bypasses_link_intent(intent):
    """With blast_context set, these link intents fall through to GENERAL."""
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


def test_show_intent_answers_during_blast_context():
    """SHOW must answer with the tour/ticket path even when a blast session is active,
    and the blast framing must NOT be injected into the show reply. Conversation
    history IS injected (as background context) so the bot never contradicts a show
    it just recommended — e.g. fan disputes "I don't see tickets for dec 4"."""
    prompt = _build_prompt(
        intent=Intent.SHOW,
        user_message="when are you coming to Cincinnati?",
        chunks=[],
        history=[{"role": "assistant", "text": "I'm watching you"}],
        blast_context=BLAST_CTX,
    )
    assert "shows or tour dates" in prompt          # SHOW path used, not GENERAL
    assert "zarnagarg.com/tickets" in prompt          # ticket link present
    assert "Webby Award" not in prompt                # blast framing NOT injected
    assert "I'm watching you" in prompt               # history present for continuity


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


def test_blast_context_prompt_is_soft_not_anchoring():
    """The blast framing must be background-only and explicitly stop hijacking
    unrelated messages — not the old 'stay anchored to this topic' instruction."""
    from app.live_shows.blast_context import build_blast_context_prompt
    out = build_blast_context_prompt("Vote for Zarna in the Webby Awards")
    assert "Vote for Zarna in the Webby Awards" in out
    low = out.lower()
    assert "background only" in low
    assert "clearly about something else" in low      # tells the model to drop it
    assert "stay anchored" not in low                  # old aggressive phrasing removed
