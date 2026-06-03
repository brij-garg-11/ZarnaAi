"""
Unit tests for the per-slug brain cache (app/brain/handler.get_brain).

We monkeypatch create_brain so no real (heavy) brain is constructed — these
tests only exercise the caching / normalisation / eviction logic.
"""

import app.brain.handler as handler


class _FakeBrain:
    def __init__(self, slug):
        self.slug = slug


def _patch_create_brain(monkeypatch):
    calls = []

    def fake_create_brain(slug=None):
        calls.append(slug)
        return _FakeBrain(slug)

    monkeypatch.setattr(handler, "create_brain", fake_create_brain)
    handler.reset_brain_cache()
    return calls


def test_same_slug_returns_cached_instance(monkeypatch):
    calls = _patch_create_brain(monkeypatch)
    b1 = handler.get_brain("alice")
    b2 = handler.get_brain("alice")
    assert b1 is b2
    assert calls == ["alice"]  # built only once


def test_different_slugs_get_different_brains(monkeypatch):
    _patch_create_brain(monkeypatch)
    a = handler.get_brain("alice")
    b = handler.get_brain("bob")
    assert a is not b
    assert a.slug == "alice"
    assert b.slug == "bob"


def test_slug_is_normalised(monkeypatch):
    calls = _patch_create_brain(monkeypatch)
    b1 = handler.get_brain("Alice")
    b2 = handler.get_brain("  alice ")
    assert b1 is b2
    assert calls == ["alice"]


def test_none_slug_maps_to_zarna(monkeypatch):
    calls = _patch_create_brain(monkeypatch)
    b = handler.get_brain(None)
    assert b.slug == "zarna"
    assert calls == ["zarna"]


def test_cache_eviction_bounds_memory(monkeypatch):
    _patch_create_brain(monkeypatch)
    monkeypatch.setattr(handler, "_BRAIN_CACHE_MAX", 2)

    handler.get_brain("a")
    handler.get_brain("b")
    # Inserting a 3rd evicts the oldest ("a").
    handler.get_brain("c")
    assert "a" not in handler._BRAIN_CACHE
    assert "b" in handler._BRAIN_CACHE
    assert "c" in handler._BRAIN_CACHE


def test_reset_clears_cache(monkeypatch):
    calls = _patch_create_brain(monkeypatch)
    handler.get_brain("alice")
    handler.reset_brain_cache()
    handler.get_brain("alice")
    assert calls == ["alice", "alice"]  # rebuilt after reset
