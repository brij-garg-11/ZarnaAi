"""
SMB AI generation with automatic provider fallback.

Call order: Gemini → OpenAI → Anthropic.
All SMB modules should call generate() instead of constructing provider
clients directly, so every call gets resilience for free.
"""

import logging

from app.config import (
    ANTHROPIC_API_KEY,
    GEMINI_API_KEY,
    GENERATION_MODEL,
    HIGH_MODEL,
    MID_MODEL,
    OPENAI_API_KEY,
)

logger = logging.getLogger(__name__)


def generate(prompt: str) -> str:
    """
    Generate text, trying providers in order until one succeeds.
    Returns an empty string if every provider fails.
    """
    if GEMINI_API_KEY:
        try:
            return _gemini(prompt)
        except Exception:
            logger.warning("SMB AI: Gemini failed, trying OpenAI next", exc_info=True)

    if OPENAI_API_KEY:
        try:
            return _openai(prompt)
        except Exception:
            logger.warning("SMB AI: OpenAI failed, trying Anthropic next", exc_info=True)

    if ANTHROPIC_API_KEY:
        try:
            return _anthropic(prompt)
        except Exception:
            logger.error("SMB AI: all providers failed", exc_info=True)

    return ""


# ---------------------------------------------------------------------------
# Provider implementations
# ---------------------------------------------------------------------------

def _gemini(prompt: str) -> str:
    from google import genai

    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(model=GENERATION_MODEL, contents=prompt)
    text = (response.text or "").strip()
    if not text:
        raise ValueError("Gemini returned empty response")
    return text


def _openai(prompt: str) -> str:
    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)
    r = client.chat.completions.create(
        model=MID_MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        temperature=0.85,
    )
    text = ((r.choices[0].message.content or "") if r.choices else "").strip()
    if not text:
        raise ValueError("OpenAI returned empty response")
    return text


def _anthropic(prompt: str) -> str:
    import anthropic

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model=HIGH_MODEL,
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )
    parts = [block.text for block in msg.content if getattr(block, "type", None) == "text"]
    text = "".join(parts).strip()
    if not text:
        raise ValueError("Anthropic returned empty response")
    return text
