"""
Source-inspection tests for the audit-leftovers PR.

Each test verifies a specific code change from the audit fixes by reading
the file content and asserting required patterns exist. We use this style
(rather than functional tests with imports) because several of the targets
live behind module-level Postgres / Stripe / Twilio bootstrap that doesn't
play nicely with pytest collection — the same approach we used in
test_blast_optout.py and test_billing_accuracy.py for prior parts.
"""

import re
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# C7 — SMB send_contact_card reads from DB, not just file
# ---------------------------------------------------------------------------

def test_send_contact_card_resolves_via_db_helper():
    src = _read("app/smb/onboarding.py")
    assert "_send_contact_card_enabled" in src, (
        "Expected helper that resolves send_contact_card from smb_bot_config DB"
    )
    # Helper must query smb_bot_config
    assert "smb_bot_config" in src, "DB-precedence helper missing smb_bot_config query"
    # The onboarding flow now calls the helper instead of reading tenant.raw directly
    assert "if _send_contact_card_enabled(conn, tenant):" in src, (
        "Onboarding flow must call _send_contact_card_enabled() to honor My Bot toggle"
    )


def test_send_contact_card_helper_falls_back_to_file_default():
    src = _read("app/smb/onboarding.py")
    # Fallback to tenant.raw on DB miss / error
    assert 'tenant.raw.get("send_contact_card", True)' in src, (
        "Helper must still respect on-disk default when DB row missing"
    )


# ---------------------------------------------------------------------------
# C8 — Notion blast metrics tenant-scoped
# ---------------------------------------------------------------------------

def test_sync_customer_costs_blast_query_filters_by_creator_slug():
    src = _read("operator/app/notion_crm.py")
    # The blast count query lives inside sync_customer_costs and must include
    # creator_slug = %s — without it every customer's Notion row gets the
    # global blast totals.
    pattern = re.compile(
        r"FROM\s+blast_drafts\s+WHERE\s+status='sent'\s+AND\s+sent_at\s+>=\s+DATE_TRUNC\('month',\s+NOW\(\)\)\s+AND\s+creator_slug\s+=\s+%s",
        re.IGNORECASE | re.DOTALL,
    )
    assert pattern.search(src), "blast_drafts query must filter by creator_slug = %s"


# ---------------------------------------------------------------------------
# H3 — Annual subscription credits multiplied by 12
# ---------------------------------------------------------------------------

def test_credits_for_cycle_helper_exists_and_doubles_annual():
    src = _read("operator/app/routes/billing.py")
    assert "def _credits_for_cycle" in src, "Missing _credits_for_cycle helper"
    # Annual must multiply by 12
    assert re.search(
        r"def _credits_for_cycle.*?base \* 12.*?annual",
        src,
        re.DOTALL,
    ), "_credits_for_cycle must return base * 12 for annual cycle"


def test_no_call_site_still_uses_plan_monthly_credits_directly():
    src = _read("operator/app/routes/billing.py")
    # No remaining `included_credits=plan.monthly_credits` — all three call
    # sites should now go through _credits_for_cycle.
    assert "included_credits=plan.monthly_credits" not in src, (
        "Found a call site still using raw plan.monthly_credits — should use _credits_for_cycle"
    )
    # And there should be at least 3 call sites using the helper.
    count = len(re.findall(r"included_credits=_credits_for_cycle\(plan, billing_cycle\)", src))
    assert count >= 3, (
        f"Expected ≥3 sites calling _credits_for_cycle, found {count}"
    )


# ---------------------------------------------------------------------------
# H4 — SlickText v2 dedup uses synthetic key
# ---------------------------------------------------------------------------

def test_slicktext_webhook_handles_v2_dedup_without_chatmessageid():
    src = _read("main.py")
    # New code must inspect payload['data'] dict shape (v2) and synthesize
    # a key from contact_id + last_message hash.
    assert 'isinstance(payload.get("data"), dict)' in src, (
        "SlickText webhook must branch on dict-shaped data (v2 payload)"
    )
    assert 'v2.get("contact_id"' in src, "v2 dedup must read contact_id"
    assert 'v2.get("last_message"' in src, "v2 dedup must read last_message"
    assert "hashlib.sha1" in src, "v2 dedup must hash the body for a stable key"
    # Helper must guard against empty dedup key (don't dedup nothing)
    assert "if message_id and _already_processed(message_id):" in src, (
        "Empty dedup key must not match — guard with `if message_id`"
    )


def test_main_imports_hashlib():
    src = _read("main.py")
    assert re.search(r"^import hashlib", src, re.MULTILINE), (
        "main.py must import hashlib for v2 dedup hashing"
    )


# ---------------------------------------------------------------------------
# H12 — Blast timezone parses with browser tz hint
# ---------------------------------------------------------------------------

def test_blast_parse_helper_exists_and_handles_tz_hint():
    src = _read("operator/app/routes/blast.py")
    assert "def _parse_local_datetime_to_utc" in src, "Missing tz parse helper"
    # Must use ZoneInfo from stdlib
    assert "from zoneinfo import ZoneInfo" in src, "Helper must use zoneinfo.ZoneInfo"
    # Falls back to UTC on missing tz string
    assert (
        "if not send_at_tz:" in src
        and "naive.replace(tzinfo=timezone.utc)" in src
    ), "Helper must fall back to UTC interpretation when tz hint missing"


def test_blast_schedule_call_sites_use_tz_helper():
    src = _read("operator/app/routes/blast.py")
    # Both schedule paths (intent=schedule and the dedicated /schedule route)
    # should call the new helper instead of replace(tzinfo=timezone.utc).
    sites = re.findall(r"_parse_local_datetime_to_utc\(send_at_str, send_at_tz\)", src)
    assert len(sites) >= 2, f"Expected ≥2 call sites to use tz helper, found {len(sites)}"


def test_blast_template_includes_tz_hidden_input():
    src = _read("operator/app/templates/blast.html")
    assert 'name="send_at_tz"' in src, (
        "blast.html must submit a send_at_tz hidden input so backend can localize"
    )
    assert "Intl.DateTimeFormat().resolvedOptions().timeZone" in src, (
        "blast.html must pull the IANA timezone from the browser at form init"
    )


# ---------------------------------------------------------------------------
# M6 — engagement docstring matches SQL
# ---------------------------------------------------------------------------

def test_engagement_docstring_no_longer_claims_click_term():
    src = _read("operator/app/engagement.py")
    # The old 4-term formula listed click_activity worth 20 points
    # but the SQL never included it. Updated docstring should:
    # (a) not claim click_activity in the live formula
    # (b) call out the 80-point cap
    body = src.split('"""')[1]  # module docstring
    assert "click_activity" not in body or "click_activity" in body and "earlier draft" in body, (
        "Docstring should explain that click_activity is NOT in the current formula"
    )
    assert "caps at 80" in body, (
        "Docstring should call out that score caps at 80 (no clicks term)"
    )


# ---------------------------------------------------------------------------
# M7 — recompute_all honors slug
# ---------------------------------------------------------------------------

def test_recompute_all_passes_slug_to_sql():
    src = _read("operator/app/engagement.py")
    # When slug is provided we expect creator_slug filters in both the
    # contacts row filter and the messages aggregation.
    assert "WHERE creator_slug = %s" in src, "messages aggregation must filter on slug"
    assert "AND c.creator_slug = %s" in src, "contacts row filter must include slug"
    # Must execute with slug bound twice.
    assert "cur.execute(sql, (slug, slug))" in src, "Must bind slug into both placeholders"


# ---------------------------------------------------------------------------
# M8 — Smart Send tenant-scoped
# ---------------------------------------------------------------------------

def test_smart_send_preview_resolves_slug_from_current_user():
    src = _read("operator/app/routes/blast.py")
    # The preview function must consult current_user() and only fall back to
    # global queries for super-admins.
    assert "def smart_send_preview" in src
    func_src = src.split("def smart_send_preview")[1].split("def ")[0]
    assert "current_user()" in func_src, "Preview must read the logged-in user"
    assert "is_super_admin" in func_src, "Preview must allow global queries only for super-admins"
    assert "AND creator_slug = %s" in func_src, "Tenant-scoped contacts query missing"
    assert "bd.creator_slug = %s" in func_src, "Tenant-scoped blast_drafts join missing"


# ---------------------------------------------------------------------------
# M9 — CSRF strict mode toggle + log
# ---------------------------------------------------------------------------

def test_operator_csrf_logs_when_origin_missing():
    src = _read("operator/app/__init__.py")
    # The CSRF check must log a warning whenever Origin/Referer is absent.
    assert "OPERATOR_CSRF_STRICT" in src, "CSRF strict mode must be env-controlled"
    assert "no Origin/Referer" in src, "Missing-origin requests must log a warning"
    # And in STRICT mode it must reject with 403.
    assert (
        'jsonify(error="CSRF check failed: missing Origin/Referer"), 403' in src
    ), "Strict mode must reject missing-origin requests with 403"


# ---------------------------------------------------------------------------
# M17 — Notion net margin reads from DB plan, not Notion
# ---------------------------------------------------------------------------

def test_notion_resolve_monthly_fee_helper_exists():
    src = _read("operator/app/notion_crm.py")
    assert "def _resolve_monthly_fee_from_db" in src, (
        "Missing helper that reads monthly_fee from operator_users + plans catalog"
    )
    # Helper must use ALL_PLANS catalog
    assert "from .billing.plans import ALL_PLANS" in src
    # Annual cycle must divide by 12
    assert "plan.annual_price_usd / 12.0" in src, (
        "Annual cycle must amortize annual_price over 12 months"
    )


def test_sync_customer_costs_calls_resolve_helper():
    src = _read("operator/app/notion_crm.py")
    assert "_resolve_monthly_fee_from_db(conn, slug)" in src, (
        "sync_customer_costs must use the new helper instead of reading from Notion"
    )


# ---------------------------------------------------------------------------
# M18 — Notion duplicate-customer check
# ---------------------------------------------------------------------------

def test_create_customer_in_notion_skips_duplicate_slug():
    src = _read("operator/app/notion_crm.py")
    func_src = src.split("def create_customer_in_notion")[1].split("\ndef ")[0]
    assert "_find_page_by_slug(database_id, slug)" in func_src, (
        "create_customer_in_notion must check for an existing page first"
    )
    assert "skipping create" in func_src, "Duplicate-skip must log a clear message"


# ---------------------------------------------------------------------------
# L4 — Footer anchor points to a real section id
# ---------------------------------------------------------------------------

def test_footer_proof_anchor_points_to_real_section():
    src = _read("lovable-frontend/src/components/Footer.tsx")
    assert '"/#proof"' in src, "Proof link should anchor /#proof (matches ProofZarna section id)"
    assert '"/#real-people"' not in src, "Old broken anchor should be removed"


def test_proof_section_actually_has_id_proof():
    src = _read("lovable-frontend/src/components/ProofZarna.tsx")
    assert 'id="proof"' in src, "ProofZarna section must keep id=\"proof\" for the footer anchor"


def test_by_the_numbers_section_has_id():
    src = _read("lovable-frontend/src/components/ByTheNumbers.tsx")
    assert 'id="by-the-numbers"' in src, "ByTheNumbers section must keep its id for footer anchor"


# ---------------------------------------------------------------------------
# L5 — Early Access dialog submits via mailto
# ---------------------------------------------------------------------------

def test_early_access_dialog_uses_mailto_not_fake_toast():
    src = _read("lovable-frontend/src/components/EarlyAccessDialog.tsx")
    assert "mailto:brij@zarnagarg.com" in src, (
        "Submit must open a mailto so the request actually reaches a person"
    )
    # Toast wording should reflect the new behavior
    assert "Opening your email" in src, "Toast copy must match the new mailto behavior"


# ---------------------------------------------------------------------------
# L7 — Flask secret default warnings
# ---------------------------------------------------------------------------

def test_main_app_warns_when_default_flask_secret_in_prod():
    src = _read("main.py")
    assert "_FLASK_SECRET_DEFAULT" in src, "main.py must keep default secret in a constant"
    assert "FLASK SECRET KEY IS USING THE HARDCODED DEFAULT" in src, (
        "main.py must error-log when running with default secret in production"
    )


def test_operator_warns_when_default_flask_secret_in_prod():
    src = _read("operator/app/__init__.py")
    assert "_OPERATOR_SECRET_DEFAULT" in src, "operator must keep default secret in a constant"
    assert "OPERATOR FLASK SECRET KEY IS USING THE HARDCODED DEFAULT" in src, (
        "operator must error-log when running with default secret in production"
    )
    assert "_looks_like_production" in src, (
        "operator must guard the warning behind a production check helper"
    )


# ---------------------------------------------------------------------------
# L8 — billing/__init__.py docstring is accurate
# ---------------------------------------------------------------------------

def test_billing_init_docstring_no_longer_references_missing_fns():
    src = _read("operator/app/billing/__init__.py")
    # Old docstring claimed enforce_send_quota and stripe_client.get_stripe
    # which never existed. Updated copy must explicitly call out their absence.
    assert "There is no `enforce_send_quota()`" in src, (
        "Docstring must call out that enforce_send_quota was never implemented"
    )
    assert "check_send_quota" in src, "Docstring must reference the actual function name"
    # Must also list the actual public surface
    for name in ("consume_credit", "get_credit_status", "seed_trial_credits", "ALL_PLANS"):
        assert name in src, f"Docstring missing reference to {name}"


# ---------------------------------------------------------------------------
# L13 — active_live_shows accepts creator_slug
# ---------------------------------------------------------------------------

def test_active_live_shows_accepts_creator_slug():
    src = _read("app/live_shows/repository.py")
    assert "def active_live_shows(creator_slug:" in src, (
        "active_live_shows must accept a creator_slug parameter"
    )
    # Falls back to env for compat
    assert 'os.getenv("CREATOR_SLUG")' in src, (
        "active_live_shows must fall back to env CREATOR_SLUG when caller passes None"
    )
    # SQL filter must exist
    assert "creator_slug = %s OR creator_slug IS NULL" in src, (
        "Filter must be backward-compatible with rows that pre-date the column"
    )
    # Tolerates UndefinedColumn for older schemas (sentinel allows test stubs)
    assert "_UndefinedColumn" in src, (
        "Must catch _UndefinedColumn (real or sentinel) so old schemas without "
        "creator_slug still work and test stubs of psycopg2 don't break import"
    )
