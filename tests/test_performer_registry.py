"""
Unit tests for app/performer/registry.py — the phone→creator routing registry.

These test the caching / TTL / miss behaviour without a live DB by
monkeypatching the single DB-touching method `_query_db`.
"""

import time

from app.performer.registry import PerformerRegistry


def _registry_with_db(mapping, call_counter):
    """Build a registry whose _query_db reads from an in-memory dict + counts calls."""
    reg = PerformerRegistry(ttl=10.0)

    def fake_query(to_number):
        call_counter.append(to_number)
        return mapping.get(to_number)

    reg._query_db = fake_query  # type: ignore[assignment]
    return reg


def test_resolves_known_number():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice"}, calls)
    assert reg.get_slug_by_to_number("+15550001111") == "alice"


def test_unknown_number_returns_none():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice"}, calls)
    assert reg.get_slug_by_to_number("+15559999999") is None


def test_none_and_blank_short_circuit_without_db():
    calls = []
    reg = _registry_with_db({}, calls)
    assert reg.get_slug_by_to_number(None) is None
    assert reg.get_slug_by_to_number("   ") is None
    assert calls == []  # never touched the DB


def test_positive_lookup_is_cached():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice"}, calls)
    reg.get_slug_by_to_number("+15550001111")
    reg.get_slug_by_to_number("+15550001111")
    reg.get_slug_by_to_number("+15550001111")
    assert len(calls) == 1  # only one DB hit despite three lookups


def test_miss_is_also_cached():
    calls = []
    reg = _registry_with_db({}, calls)
    reg.get_slug_by_to_number("+15559999999")
    reg.get_slug_by_to_number("+15559999999")
    assert len(calls) == 1  # negative result cached too


def test_cache_expires_after_ttl():
    calls = []
    reg = PerformerRegistry(ttl=0.05)
    mapping = {"+15550001111": "alice"}

    def fake_query(to_number):
        calls.append(to_number)
        return mapping.get(to_number)

    reg._query_db = fake_query  # type: ignore[assignment]

    reg.get_slug_by_to_number("+15550001111")
    time.sleep(0.08)
    reg.get_slug_by_to_number("+15550001111")
    assert len(calls) == 2  # re-queried after TTL elapsed


def test_newly_provisioned_number_appears_after_refresh():
    calls = []
    mapping = {}
    reg = PerformerRegistry(ttl=0.05)

    def fake_query(to_number):
        calls.append(to_number)
        return mapping.get(to_number)

    reg._query_db = fake_query  # type: ignore[assignment]

    # Number not provisioned yet → None
    assert reg.get_slug_by_to_number("+15550002222") is None
    # Provision it (simulating the operator buying it)
    mapping["+15550002222"] = "bob"
    time.sleep(0.08)
    # After the TTL window it becomes routable
    assert reg.get_slug_by_to_number("+15550002222") == "bob"


def test_invalidate_single_number_forces_requery():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice"}, calls)
    reg.get_slug_by_to_number("+15550001111")
    reg.invalidate("+15550001111")
    reg.get_slug_by_to_number("+15550001111")
    assert len(calls) == 2


def test_invalidate_all_clears_cache():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice", "+15550002222": "bob"}, calls)
    reg.get_slug_by_to_number("+15550001111")
    reg.get_slug_by_to_number("+15550002222")
    reg.invalidate()
    reg.get_slug_by_to_number("+15550001111")
    assert len(calls) == 3


def test_whitespace_normalised_on_lookup():
    calls = []
    reg = _registry_with_db({"+15550001111": "alice"}, calls)
    assert reg.get_slug_by_to_number("  +15550001111  ") == "alice"
