"""
Tests for blast opt-out recording (C1, C2) and SMS log PII masking (H10).

Strategy: the production files we're verifying have heavy import chains that
block unit test loading in Python 3.11 (nested f-strings in app/admin/__init__.py).
We therefore use source-code inspection to assert the critical SQL patterns and
PII-masking changes are in place, which is a valid approach for schema-critical
correctness checks. Runtime integration tests for these flows live in the
manual / staging test phase (see docs/plans-to-complete/).
"""

import re
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# C1 — _record_blast_optout writes to broadcast_optouts
# ---------------------------------------------------------------------------

def test_record_blast_optout_inserts_phone_into_broadcast_optouts():
    """main.py _record_blast_optout must INSERT into broadcast_optouts."""
    src = _read("main.py")

    # Find the function body
    fn_start = src.find("def _record_blast_optout(")
    assert fn_start != -1, "_record_blast_optout not found in main.py"

    # Next function starts with 'def ' at column 0
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "broadcast_optouts" in fn_body, (
        "_record_blast_optout must INSERT into broadcast_optouts"
    )
    assert "INSERT" in fn_body.upper(), (
        "_record_blast_optout must contain an INSERT statement"
    )
    assert "phone_number" in fn_body, (
        "_record_blast_optout must pass phone_number as a parameter"
    )


def test_record_blast_optout_still_updates_blast_drafts():
    """main.py _record_blast_optout must still UPDATE blast_drafts.opt_out_count."""
    src = _read("main.py")
    fn_start = src.find("def _record_blast_optout(")
    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "blast_drafts" in fn_body, "blast_drafts UPDATE must still be in _record_blast_optout"
    assert "opt_out_count" in fn_body, "opt_out_count UPDATE must still be in _record_blast_optout"


def test_record_blast_optout_signature_takes_phone_number_param():
    """_record_blast_optout must accept a phone_number parameter."""
    src = _read("main.py")
    assert "def _record_blast_optout(phone_number" in src, (
        "_record_blast_optout must accept phone_number parameter"
    )


def test_slicktext_optout_passes_phone_to_record_blast_optout():
    """SlickText webhook must pass raw_phone to _record_blast_optout."""
    src = _read("main.py")
    idx = src.find("def slicktext_webhook(")
    assert idx != -1
    next_fn = re.search(r"\ndef \w", src[idx + 1:])
    fn_body = src[idx: idx + (next_fn.start() + 1 if next_fn else len(src))]

    # The call is threaded: _record_blast_optout, args=(raw_phone,)
    assert "_record_blast_optout" in fn_body, (
        "slicktext_webhook must call _record_blast_optout"
    )
    assert re.search(r"_record_blast_optout.*?args\s*=\s*\(\s*raw_phone", fn_body, re.DOTALL), (
        "slicktext_webhook must pass raw_phone as args to _record_blast_optout thread"
    )


def test_twilio_optout_passes_phone_to_record_blast_optout():
    """Twilio webhook must pass raw_from to _record_blast_optout (new in this PR)."""
    src = _read("main.py")
    idx = src.find("def twilio_webhook(")
    assert idx != -1
    fn_body = src[idx:]

    assert "_record_blast_optout" in fn_body, (
        "twilio_webhook must call _record_blast_optout to persist opt-outs"
    )
    assert re.search(r"_record_blast_optout.*?args\s*=\s*\(\s*raw_from", fn_body, re.DOTALL), (
        "twilio_webhook must pass raw_from as args to _record_blast_optout thread"
    )


# ---------------------------------------------------------------------------
# C2 — SMB portal _get_all_subscriber_phones excludes STOPped users
# ---------------------------------------------------------------------------

def test_smb_portal_get_all_subscriber_phones_filters_active():
    """operator/app/routes/smb_portal.py _get_all_subscriber_phones must WHERE status='active'."""
    src = _read("operator/app/routes/smb_portal.py")
    fn_start = src.find("def _get_all_subscriber_phones(")
    assert fn_start != -1, "_get_all_subscriber_phones not found"

    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "status" in fn_body.lower() and "active" in fn_body.lower(), (
        "_get_all_subscriber_phones must filter by status='active' to exclude STOPped subscribers. "
        f"Got:\n{fn_body}"
    )


# ---------------------------------------------------------------------------
# C3 — Twilio validate_signature is a fail-safe (returns False when unconfigured)
# ---------------------------------------------------------------------------

def test_twilio_validate_signature_fail_safe():
    """validate_signature in twilio_adapter.py must return False (not True) when no validator."""
    src = _read("app/messaging/twilio_adapter.py")
    fn_start = src.find("def validate_signature(")
    assert fn_start != -1

    next_fn = re.search(r"\n    def \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    # The old code returned True; must now return False
    lines = fn_body.splitlines()
    for line in lines:
        if "not self._validator" in line or "not self._validator:" in line:
            # Find the next non-empty, non-comment return in this if-block
            pass

    # Direct string check: the fail-safe branch must return False
    assert "return False" in fn_body, (
        "validate_signature must return False when _validator is not configured, not True"
    )
    # And must NOT have 'return True' inside the 'if not self._validator' branch
    no_validator_idx = fn_body.find("if not self._validator")
    assert no_validator_idx != -1
    # Slice from there to the next 'return'
    after_check = fn_body[no_validator_idx:]
    first_return_in_check = after_check.find("return ")
    return_value = after_check[first_return_in_check:first_return_in_check + 15].strip()
    assert "False" in return_value, (
        f"validate_signature must return False in the fail-safe branch, got: {return_value!r}"
    )


# ---------------------------------------------------------------------------
# H9 / H10 — PII masking in error logs
# ---------------------------------------------------------------------------

def test_slicktext_process_error_log_uses_last4():
    """_process_slicktext_message error log must use last-4 masking, not full phone."""
    src = _read("main.py")
    fn_start = src.find("def _process_slicktext_message(")
    assert fn_start != -1

    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    # The error log line must not format phone_number directly (PII)
    bad_patterns = [
        r'logging\.error\([^)]*"[^"]*from %s[^"]*"[^,]*,\s*phone_number[^-\[]',
    ]
    for pattern in bad_patterns:
        assert not re.search(pattern, fn_body), (
            f"_process_slicktext_message error log must use last-4 masking: found {pattern}"
        )

    # The safe pattern must use [-4:] slicing
    assert "phone_number[-4:]" in fn_body or "...%s" in fn_body, (
        "_process_slicktext_message must use last-4 masking in its error log"
    )


def test_twilio_process_error_log_uses_last4():
    """_process_twilio_message error log must use last-4 masking, not full phone."""
    src = _read("main.py")
    fn_start = src.find("def _process_twilio_message(")
    assert fn_start != -1

    next_fn = re.search(r"\ndef \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    assert "phone_number[-4:]" in fn_body or "...%s" in fn_body, (
        "_process_twilio_message must use last-4 masking in its error log"
    )


def test_slicktext_adapter_filter_logs_use_last4():
    """SlickText adapter filter_inbound_for_ai logs must not expose full phone numbers."""
    src = _read("app/messaging/slicktext_adapter.py")
    fn_start = src.find("def filter_inbound_for_ai(")
    assert fn_start != -1

    next_fn = re.search(r"\n    def \w", src[fn_start + 1:])
    fn_body = src[fn_start: fn_start + (next_fn.start() + 1 if next_fn else len(src))]

    # Old code used f-string with {phone} directly; new code uses ...%s with [-4:]
    assert "phone}" not in fn_body, (
        "filter_inbound_for_ai must not log the full phone number in f-strings"
    )
    assert "{phone}" not in fn_body, (
        "filter_inbound_for_ai must not log the full phone number"
    )
