"""Version-safe Gemini thinking-config control for latency-sensitive calls.

``gemini-flash-latest`` is a floating alias. In Jul 2026 Google rolled it from
Gemini 2.5 Flash to ``gemini-3.6-flash``, a reasoning model that defaults to
high/dynamic thinking — every generate_content call made without an explicit
thinking config started spending seconds on hidden reasoning tokens (prod
gen_ms went from ~1-2s to 4-5s overnight, with no code change on our side).
The voice path was patched in #77; this module gives the SMS text path
(reply generation, intent classification, complexity routing, fan-memory
extraction) the same protection, from one shared place.

In Oct 2026 the alias rolled again, to ``gemini-3.8-flash``, which rejects
``thinking_level="minimal"`` with 400 INVALID_ARGUMENT. The original fallback
here (retry config-less on every call) silently put the whole SMS path back on
full dynamic thinking — billed as output tokens but invisible to our cost
logging — and tripled daily Gemini spend. So rejection handling is now:

  1. Retry once with ``thinking_level="low"`` (the cheapest level every
     Gemini 3.x generation so far has accepted).
  2. Only if that is also rejected, fall back to a config-less call.
  3. Cache the outcome per (model, config) so later calls go straight to
     whatever worked — never a doomed request per reply again.

Transient errors (timeouts, 5xx) keep the old behaviour: one config-less
retry, nothing cached.

Model-generation differences this has to absorb:
  - Gemini 3.x accept ``thinking_level`` and reject ``thinking_budget=0``
    with 400 INVALID_ARGUMENT; 3.8 additionally rejects level "minimal".
  - Gemini 2.5 accept a numeric ``thinking_budget`` (0 disables thinking)
    and predate ``thinking_level``.
  - A future alias roll can change the accepted knobs again, so every request
    made through here degrades gracefully rather than failing.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_VALID_LEVELS = frozenset({"minimal", "low", "medium", "high"})
# "off" (and friends) = operator explicitly wants the model's default thinking
# behaviour — the instant, env-only rollback path if a bounded level ever
# causes a reply-quality problem in prod.
_DISABLED_VALUES = frozenset({"", "off", "none", "default"})

# Level a rejected config is downgraded to before giving up on bounding
# thinking entirely. Measured on gemini-3.8-flash (Oct 2026): "minimal" is
# rejected, "low" is accepted and produced zero thought tokens on the live
# SMS prompt — so "low" is both the cheapest accepted bound and the safety net.
_FALLBACK_LEVEL = "low"

# id(config) -> level string it encodes. Callers hold their configs as module
# constants, so the ids stay valid for the life of the process.
_config_levels: dict = {}
# (model, id(original config)) -> replacement config (or None = config-less).
# Populated only when a model rejects a config outright, so every later call
# skips the request we already know will fail.
_config_overrides: dict = {}
# Lazy singleton for the downgrade config — pinned here so its id stays valid.
_fallback_config_cache: list = []


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
            config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(**thinking_kwargs)
            )
            _config_levels[id(config)] = level
            return config
        except Exception:
            continue
    return None


def clear_thinking_caches() -> None:
    """Forget cached downgrades. Test hook — prod never needs this."""
    _config_levels.clear()
    _config_overrides.clear()
    _fallback_config_cache.clear()


def _get_fallback_config():
    if not _fallback_config_cache:
        _fallback_config_cache.append(build_thinking_config(_FALLBACK_LEVEL))
    return _fallback_config_cache[0]


def _looks_like_rejected_config(exc: Exception) -> bool:
    """True when the error says the thinking config itself is unsupported.

    Gemini surfaces these as 400 INVALID_ARGUMENT (e.g. "Thinking level
    MINIMAL is not supported for this model"). Anything else is treated as
    transient and must not trigger a cached downgrade.
    """
    text = str(exc)
    return "INVALID_ARGUMENT" in text or "thinking" in text.lower()


def generate_with_thinking(client, model: str, contents: str, config):
    """``generate_content`` with ``config``, degrading gracefully if rejected.

    Rejected config (400 INVALID_ARGUMENT / unsupported thinking level):
    retry once at ``_FALLBACK_LEVEL``, then config-less, caching whichever
    worked so subsequent calls skip the doomed attempt entirely. Transient
    errors: one config-less retry, nothing cached (original #78 behaviour).
    Exceptions from the final config-less attempt propagate so callers keep
    their existing error handling (fallback text, Intent.GENERAL, ...).
    """
    if config is None:
        return client.models.generate_content(model=model, contents=contents)

    key = (model, id(config))
    active = _config_overrides[key] if key in _config_overrides else config
    if active is None:  # cached: this model accepts no thinking config at all
        return client.models.generate_content(model=model, contents=contents)

    try:
        return client.models.generate_content(
            model=model, contents=contents, config=active
        )
    except Exception as exc:
        rejected = _looks_like_rejected_config(exc)
        logger.warning(
            "[ZARNA] Gemini call with thinking config failed for model=%s (%s)%s",
            model,
            exc,
            " — downgrading" if rejected else " — retrying without config",
        )
        if not rejected:
            return client.models.generate_content(model=model, contents=contents)

    # The model rejected the config outright — e.g. an alias roll-forward made
    # the configured level unsupported (gemini-3.8-flash vs "minimal", Oct 2026).
    fallback = None
    if _config_levels.get(id(active)) != _FALLBACK_LEVEL:
        fallback = _get_fallback_config()

    if fallback is not None:
        try:
            response = client.models.generate_content(
                model=model, contents=contents, config=fallback
            )
            _config_overrides[key] = fallback
            logger.warning(
                "[ZARNA] model=%s: thinking config downgraded to %r and cached",
                model,
                _FALLBACK_LEVEL,
            )
            return response
        except Exception as exc2:
            logger.warning(
                "[ZARNA] model=%s rejected %r thinking config too (%s) — "
                "going config-less",
                model,
                _FALLBACK_LEVEL,
                exc2,
            )
            if _looks_like_rejected_config(exc2):
                _config_overrides[key] = None
    else:
        # Already at the fallback level (or no config buildable) — stop
        # sending a config this model is known to reject.
        _config_overrides[key] = None

    return client.models.generate_content(model=model, contents=contents)
