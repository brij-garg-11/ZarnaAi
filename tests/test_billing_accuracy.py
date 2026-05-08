"""
Tests for Part 3 — Billing Accuracy fixes.

Covers:
- Stripe webhook idempotency table + claim/release helpers
- Performer AI credit gate gated behind BILLING_HARD_GATE env var (fail-open)
- Dashboard "Messages Today" → "Last 24 Hours" label fix
"""

import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# H1 — Stripe webhook idempotency
# ---------------------------------------------------------------------------

def test_stripe_webhook_events_table_migration_exists():
    """operator/app/db.py must include CREATE TABLE stripe_webhook_events."""
    src = _read("operator/app/db.py")
    assert re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+stripe_webhook_events",
        src,
        re.IGNORECASE,
    ), "operator/app/db.py must register the stripe_webhook_events table"


def test_stripe_webhook_events_has_event_id_primary_key():
    """The table must use event_id as PRIMARY KEY for ON CONFLICT-based dedup."""
    src = _read("operator/app/db.py")
    # Find the table definition
    match = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+stripe_webhook_events\s*\((.*?)\)",
        src,
        re.IGNORECASE | re.DOTALL,
    )
    assert match, "stripe_webhook_events CREATE TABLE not found"
    body = match.group(1).lower()
    assert "event_id" in body and "primary key" in body, (
        f"stripe_webhook_events must declare event_id PRIMARY KEY. Got:\n{body}"
    )


def test_stripe_webhook_uses_idempotency_claim():
    """stripe_webhook handler must call _claim_stripe_event before processing."""
    src = _read("operator/app/routes/billing.py")

    # Find the webhook function
    fn_start = src.find("def stripe_webhook(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "_claim_stripe_event" in fn_body, (
        "stripe_webhook must call _claim_stripe_event(event_id, etype) before invoking handlers"
    )

    # The duplicate path must return 200 (jsonify(received=True)) so Stripe stops retrying
    assert re.search(
        r"_claim_stripe_event.*?duplicate",
        fn_body,
        re.DOTALL,
    ), "stripe_webhook must short-circuit and return 200 for duplicate events"


def test_stripe_webhook_releases_claim_on_handler_failure():
    """If handler raises, the claim must be released so Stripe retry can re-process."""
    src = _read("operator/app/routes/billing.py")
    fn_start = src.find("def stripe_webhook(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "_release_stripe_event" in fn_body, (
        "On handler exception, stripe_webhook must call _release_stripe_event(event_id) "
        "so the next Stripe retry is processed normally instead of being silently dropped"
    )


def test_claim_helper_uses_on_conflict_do_nothing():
    """_claim_stripe_event must use INSERT ... ON CONFLICT DO NOTHING for atomicity."""
    src = _read("operator/app/routes/billing.py")
    fn_start = src.find("def _claim_stripe_event(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "ON CONFLICT" in fn_body.upper() and "DO NOTHING" in fn_body.upper(), (
        "_claim_stripe_event must use INSERT ... ON CONFLICT DO NOTHING for atomic dedup"
    )
    # Must check rowcount to distinguish first insert (1) from duplicate (0)
    assert "rowcount" in fn_body, (
        "_claim_stripe_event must check cur.rowcount > 0 to detect duplicates"
    )


# ---------------------------------------------------------------------------
# H2 — Performer AI credit gate
# ---------------------------------------------------------------------------

def test_has_credits_remaining_function_exists():
    """main.py must define _has_credits_remaining(slug) gating helper."""
    src = _read("main.py")
    assert "def _has_credits_remaining(" in src, (
        "main.py must define _has_credits_remaining(slug) — used to short-circuit "
        "AI replies when a client has run out of credits"
    )


def test_credit_gate_is_env_var_controlled():
    """Hard credit gate must be opt-in via BILLING_HARD_GATE env var (default fail-open)."""
    src = _read("main.py")
    fn_start = src.find("def _has_credits_remaining(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "BILLING_HARD_GATE" in fn_body, (
        "_has_credits_remaining must check BILLING_HARD_GATE env var so the change "
        "is opt-in (zero behavior change in prod until explicitly enabled)"
    )

    # Must short-circuit to True when the gate is off (fail-open default)
    assert "return True" in fn_body, (
        "_has_credits_remaining must default to True (fail-open) when gate is off"
    )


def test_credit_gate_handles_unlimited_tiers():
    """grandfathered/founder/internal tiers must always return True (no metering)."""
    src = _read("main.py")
    fn_start = src.find("def _has_credits_remaining(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    for tier in ("grandfathered", "founder", "internal"):
        assert tier in fn_body, (
            f"_has_credits_remaining must whitelist the '{tier}' plan tier"
        )


def test_credit_gate_called_from_slicktext_handler():
    """_process_slicktext_message must call _has_credits_remaining BEFORE handle_incoming_message."""
    src = _read("main.py")
    fn_start = src.find("def _process_slicktext_message(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "_has_credits_remaining" in fn_body, (
        "_process_slicktext_message must check credits before calling brain.handle_incoming_message"
    )

    # Order check: _has_credits_remaining must come before handle_incoming_message
    gate_idx = fn_body.find("_has_credits_remaining")
    reply_idx = fn_body.find("handle_incoming_message")
    assert gate_idx < reply_idx, (
        "_has_credits_remaining must be called BEFORE handle_incoming_message — "
        "otherwise we generate (and pay for) a reply we then refuse to send"
    )


def test_credit_gate_called_from_twilio_handler():
    """_process_twilio_message must call _has_credits_remaining BEFORE handle_incoming_message."""
    src = _read("main.py")
    fn_start = src.find("def _process_twilio_message(")
    assert fn_start != -1
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "_has_credits_remaining" in fn_body, (
        "_process_twilio_message must check credits before generating a reply"
    )

    gate_idx = fn_body.find("_has_credits_remaining")
    reply_idx = fn_body.find("handle_incoming_message")
    assert gate_idx < reply_idx, (
        "_has_credits_remaining must precede handle_incoming_message in Twilio path"
    )


def test_credit_gate_emits_ops_metric_on_block():
    """When credits are exhausted, ops_bump('ai_reply_credit_exhausted') must fire for monitoring."""
    src = _read("main.py")
    assert 'ai_reply_credit_exhausted' in src, (
        "Credit-blocked replies must emit ops_bump('ai_reply_credit_exhausted') so we can "
        "monitor blocks in dashboards / alerts"
    )


# ---------------------------------------------------------------------------
# M22 — Dashboard label fix
# ---------------------------------------------------------------------------

def test_operator_dashboard_label_says_24h_not_today():
    """operator/app/templates/dashboard.html must say 'Last 24 Hours', not 'Messages Today'."""
    src = _read("operator/app/templates/dashboard.html")
    assert "Messages Today" not in src, (
        "Dashboard label must not say 'Messages Today' — query is rolling 24h, not calendar day"
    )
    assert "Last 24 Hours" in src or "Last 24 hours" in src, (
        "Dashboard label must say 'Last 24 Hours' to match the rolling-window query"
    )


def test_admin_dashboard_label_says_24h_not_today():
    """app/admin/__init__.py admin dashboard HTML must use 'Last 24 Hours' label.

    The variable name `messages_today` is unchanged for backward compatibility,
    but the user-facing label in the stat card must match the rolling-24h query.
    """
    src = _read("app/admin/__init__.py")

    # The stat card has structure:
    #   <div class="stat-label">...</div>
    #   <div class="stat-value teal">{stats["messages_today"]:,}</div>
    # Find the assignment in the HTML and check the label that immediately precedes it
    html_match = re.search(
        r'<div class="stat-label">([^<]+)</div>\s*<div class="stat-value teal">\{stats\["messages_today"\]',
        src,
    )
    assert html_match, (
        "Could not locate the messages_today stat card in admin HTML — "
        "label format may have changed"
    )
    label = html_match.group(1).strip()
    assert label != "Messages Today", (
        "Admin dashboard label must not say 'Messages Today' for the rolling-24h query"
    )
    assert "24" in label, (
        f"Admin dashboard label must reference '24' (hours) to match query. Got: {label!r}"
    )
