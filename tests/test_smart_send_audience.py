"""
Source-inspection tests for the cadence-aware "smart" blast audience.

Smart Send must resolve to every fan in a known tier, minus opt-outs, minus
anyone already blasted inside their tier's cadence window — i.e. exactly the
set the /api/blasts/smart-send-preview endpoint counts. We assert this via
source inspection (same approach as test_audit_leftovers.py) because
operator/app/queries.py sits behind a module-level Postgres bootstrap that
doesn't play nicely with pytest collection.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# queries.py — cadence CASE constant must mirror api.py CADENCE_DAYS
# ---------------------------------------------------------------------------

def test_smart_cadence_case_matches_preview_windows():
    src = _read("operator/app/queries.py")
    assert "_SMART_CADENCE_CASE" in src, "Missing shared Smart Send cadence CASE constant"
    case = src.split("_SMART_CADENCE_CASE", 1)[1].split('"""', 2)[1]
    # Per-tier windows must match CADENCE_DAYS in routes/api.py exactly so the
    # previewed "sending" count equals who actually receives the blast.
    assert "WHEN 'superfan' THEN INTERVAL '5 days'" in case
    assert "WHEN 'engaged'  THEN INTERVAL '7 days'" in case
    assert "WHEN 'lurker'   THEN INTERVAL '14 days'" in case
    assert "WHEN 'dormant'  THEN INTERVAL '30 days'" in case


def test_api_cadence_days_unchanged():
    # Guards the invariant the constant above is mirroring.
    src = _read("operator/app/routes/api.py")
    assert (
        'CADENCE_DAYS = {"superfan": 5, "engaged": 7, "lurker": 14, "dormant": 30}'
        in src
    ), "smart audience cadence is keyed off api.py CADENCE_DAYS — keep them in sync"


# ---------------------------------------------------------------------------
# queries.py — both count and phone resolution implement the smart branch
# ---------------------------------------------------------------------------

def _smart_branch(func_src: str) -> str:
    assert 'audience_type == "smart"' in func_src, "Missing smart audience branch"
    return func_src.split('audience_type == "smart"', 1)[1]


def test_count_audience_smart_branch_excludes_optouts_and_recent_blasts():
    src = _read("operator/app/queries.py")
    func = src.split("def count_audience", 1)[1].split("\ndef ", 1)[0]
    branch = _smart_branch(func)
    assert "NOT IN (SELECT phone_number FROM broadcast_optouts)" in branch, (
        "Count must subtract opt-outs to match the preview total"
    )
    assert "NOT EXISTS" in branch and "blast_recipients" in branch, (
        "Count must exclude fans blasted within the cadence window"
    )
    assert "_SMART_CADENCE_CASE" in branch
    assert "fan_tier IN ('superfan','engaged','lurker','dormant')" in branch


def test_get_audience_phones_smart_branch_filters_cadence_and_tenant():
    src = _read("operator/app/queries.py")
    func = src.split("def get_audience_phones", 1)[1].split("\ndef ", 1)[0]
    branch = _smart_branch(func)
    assert "NOT EXISTS" in branch and "blast_recipients" in branch
    assert "blast_drafts bd" in branch, "Cadence join must reach blast_drafts for tenant scoping"
    assert "_SMART_CADENCE_CASE" in branch
    # Tenant scoping on both the contacts row and the cadence join.
    assert '_slug_clause(creator_slug, "c.")' in branch
    assert '_slug_clause(creator_slug, "bd.")' in branch
    # WhatsApp pseudo-numbers are never blasted.
    assert "NOT LIKE 'whatsapp:%%'" in branch
    # Opt-outs are removed by the shared optout_set filter after the branch.
    assert "optout_set" in func


# ---------------------------------------------------------------------------
# routes/api.py — every audience_type gate accepts "smart"
# ---------------------------------------------------------------------------

def test_all_blast_audience_gates_allow_smart():
    src = _read("operator/app/routes/api.py")
    gates = re.findall(r'audience_type not in \(([^)]*)\)', src)
    # save, preview-count, and send each guard the audience_type.
    assert len(gates) >= 3, f"Expected >=3 audience_type gates, found {len(gates)}"
    for g in gates:
        assert '"smart"' in g, f'audience_type gate missing "smart": {g}'
