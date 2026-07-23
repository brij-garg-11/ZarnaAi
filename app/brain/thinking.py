"""Version-safe Gemini thinking-config control for latency-sensitive calls.

``gemini-flash-latest`` is a floating alias. In Jul 2026 Google rolled it from
Gemini 2.5 Flash to ``gemini-3.6-flash``, a reasoning model that defaults to
high/dynamic thinking — every generate_content call made without an explicit
thinking config started spending seconds on hidden reasoning tokens (prod
gen_ms went from ~1-2s to 4-5s overnight, with no code change on our side).
The voice path was patched in #77; this module gives the SMS text path
(reply generation, intent classification, complexity routing) the same
protection, from one shared place.

Model-generation differences this has to absorb:
  - Gemini 3.x accept ``thinking_level`` (minimal|low|medium|high) and reject
    ``thinking_budget=0`` with 400 INVALID_ARGUMENT.
  - Gemini 2.5 accept a numeric ``thinking_budget`` (0 disables thinking) and
    predate ``thinking_level``.
  - A future alias roll can change the accepted knobs again, so every request
    made through here falls back to a config-less call rather than failing.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_VALID_LEVELS = frozenset({"minimal", "low", "medium", "high"})
# "off" (and friends) = operator explicitly wants the model's default thinking
# behaviour — the instant, env-only rollback path if a bounded level ever
# causes a reply-quality problem in prod.
_DISABLED_VALUES = frozenset({"", "off", "none", "default"})


def build_thinking_config(level: str, fallback_budget: int = 128):
    """Build a GenerateContentConfig bounding hidden reasoning to ``level``.

    Returns ``None`` (callers then send no config at all) when the level is
    "off"/empty or the installed google-genai predates ThinkingConfig.

    Prefers the Gemini 3.x ``thinking_level`` knob and falls back to a small
    positive ``thinking_budget`` for older SDKs. Never emits
    ``thinking_budget=0`` — Gemini 3.x reject that with 400 INVALID_ARGUMENT
    (the exact failure that broke the voice line before #77).
    """
    level = (level or "").strip().lower()
    if level in _DISABLED_VALUES:
        return None
    if level not in _VALID_LEVELS:
        logger.warning(
            "[ZARNA] unknown thinking level %r — using 'low' instead", level
        )
        level = "low"

    try:
        from google.genai import types
    except Exception:  # SDK too old — caller proceeds without a config
        return None

    for thinking_kwargs in (
        {"thinking_level": level},
        {"thinking_budget": max(1, int(fallback_budget))},
    ):
        try:
            return types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(**thinking_kwargs)
            )
        except Exception:
            continue
    return None


def generate_with_thinking(client, model: str, contents: str, config):
    """``generate_content`` with ``config``; retry once without it if rejected.

    A floating-alias roll-forward can invalidate a previously-valid thinking
    config (that is exactly what took down voice before #77). One config-less
    retry means the worst case is the pre-fix latency — never a dead reply.
    Exceptions from the config-less attempt propagate so callers keep their
    existing error handling (fallback text, Intent.GENERAL, tier=medium, ...).
    """
    if config is not None:
        try:
            return client.models.generate_content(
                model=model, contents=contents, config=config
            )
        except Exception as exc:
            logger.warning(
                "[ZARNA] Gemini call with thinking config failed for model=%s "
                "(%s) — retrying without config",
                model,
                exc,
            )
    return client.models.generate_content(model=model, contents=contents)
