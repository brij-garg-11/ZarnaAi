"""
Tests for Part 4 — Frontend Polish fixes.

Covers (source-inspection — frontend has no Vitest setup yet):
- /how-it-works/business redirect preserves audience state
- ForgotPassword surfaces API errors instead of always showing success
- CreditsWidget treats total === 0 as "out of credits" not "Unlimited"
- FanHistoryTable / FanOfTheWeek / CustomerHistoryTable navigate with tenant prefix
- Tier naming: API returns 'lurker' (matches DB), frontend dictionaries include both keys
- UserMenu has a Billing menu item
- DashboardHeader business nav uses labels.audienceNav not hardcoded "Audience"
"""

import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _read(rel_path: str) -> str:
    with open(os.path.join(PROJECT_ROOT, rel_path)) as f:
        return f.read()


# ---------------------------------------------------------------------------
# /how-it-works/business redirect preserves audience state
# ---------------------------------------------------------------------------

def test_how_it_works_business_redirect_passes_audience_state():
    """The redirect must include state={{ audience: 'business' }} so deep links work."""
    src = _read("lovable-frontend/src/App.tsx")

    # Find the /how-it-works/business route
    match = re.search(
        r'path="/how-it-works/business"\s*element=\{<Navigate[^>]*?>\s*\}',
        src,
    )
    if not match:
        # Multi-line variant
        idx = src.find('path="/how-it-works/business"')
        assert idx != -1, "/how-it-works/business route not found"
        snippet = src[idx: idx + 400]
    else:
        snippet = match.group(0)

    assert "state=" in snippet and "business" in snippet, (
        "/how-it-works/business redirect must pass state={ audience: 'business' } "
        f"so deep links render the business variant. Got:\n{snippet}"
    )


# ---------------------------------------------------------------------------
# ForgotPassword surfaces API errors
# ---------------------------------------------------------------------------

def test_forgot_password_does_not_swallow_errors():
    """ForgotPassword must not unconditionally show success on API failure."""
    src = _read("lovable-frontend/src/pages/ForgotPassword.tsx")

    # The previous code had `} catch { /* swallow */ } finally { setSubmitted(true); }`
    # which always shows success. New code must check res.ok and surface errors.
    assert "res.ok" in src or "res.status" in src or "response.ok" in src, (
        "ForgotPassword must check the response status and not unconditionally show success"
    )

    # Must have an error state variable
    assert re.search(r"\[error,\s*setError\]", src) or "useState<string | null>" in src, (
        "ForgotPassword must track an error state (useState) to surface API failures"
    )


def test_forgot_password_renders_error_to_user():
    """The error must be rendered (role='alert') so screen readers + sighted users see it."""
    src = _read("lovable-frontend/src/pages/ForgotPassword.tsx")
    assert 'role="alert"' in src or "role='alert'" in src, (
        "ForgotPassword must render the error with role='alert' for accessibility"
    )


# ---------------------------------------------------------------------------
# Credits widget zero-state
# ---------------------------------------------------------------------------

def test_credits_widget_zero_total_is_not_unlimited():
    """CreditsWidget/CreditsChip must NOT treat total === 0 as Unlimited."""
    src = _read("lovable-frontend/src/components/shell/CreditsWidget.tsx")

    # Find the isUnlimited expression(s). The previous bug was:
    #   total === null || total === undefined || total === 0
    # New code must NOT include `total === 0` in the unlimited check.
    unlimited_exprs = re.findall(r"isUnlimited\s*=[^;]+", src)
    assert unlimited_exprs, "Could not locate isUnlimited assignments"

    for expr in unlimited_exprs:
        assert "total === 0" not in expr and "total===0" not in expr, (
            "isUnlimited must NOT consider total === 0 as unlimited — that's a real "
            f"'no plan / 0 credits' state. Got: {expr}"
        )


def test_credits_chip_shows_no_credits_for_zero_total():
    """When total is 0 (and not flagged unlimited), chip must render an out-of-credits hint."""
    src = _read("lovable-frontend/src/components/shell/CreditsWidget.tsx")
    # Should display "No credits" or similar prominent text when totalNum === 0
    assert re.search(r"No credits|Out of credits|0 credits", src, re.IGNORECASE), (
        "CreditsChip must show a clear 'No credits' state when quota is 0"
    )


# ---------------------------------------------------------------------------
# Fan/Customer nav paths use tenant prefix (sp())
# ---------------------------------------------------------------------------

def test_fan_history_table_uses_slug_path_for_inbox_nav():
    """FanHistoryTable must wrap /inbox/${last4} with sp() so URL is tenant-scoped."""
    src = _read("lovable-frontend/src/components/dashboard/FanHistoryTable.tsx")
    assert "useSlugPath" in src, "FanHistoryTable must import useSlugPath"
    assert re.search(
        r"navigate\s*\(\s*sp\s*\(\s*`/inbox/\$\{last4\}`",
        src,
    ), "FanHistoryTable navigate must wrap path with sp() — was hardcoded /inbox/{last4}"


def test_fan_of_the_week_uses_slug_path():
    """FanOfTheWeek navigate must use sp() for inbox link."""
    src = _read("lovable-frontend/src/components/dashboard/FanOfTheWeek.tsx")
    assert "useSlugPath" in src, "FanOfTheWeek must import useSlugPath"
    assert re.search(r"navigate\s*\(\s*sp\s*\(", src), (
        "FanOfTheWeek must wrap navigate path with sp()"
    )


def test_customer_history_table_uses_slug_path():
    """CustomerHistoryTable navigate must use sp() for inbox link."""
    src = _read("lovable-frontend/src/components/dashboard/CustomerHistoryTable.tsx")
    assert "useSlugPath" in src, "CustomerHistoryTable must import useSlugPath"
    assert re.search(
        r"navigate\s*\(\s*sp\s*\(\s*`/inbox/\$\{last4\}`",
        src,
    ), "CustomerHistoryTable navigate must wrap path with sp()"


# ---------------------------------------------------------------------------
# Tier naming consistency
# ---------------------------------------------------------------------------

def test_api_tier_order_uses_lurker_not_casual():
    """operator/app/routes/api.py /api/audience must return 'lurker' (DB value), not 'casual'."""
    src = _read("operator/app/routes/api.py")

    # Find tier_order assignment near the /api/audience response
    matches = re.findall(r"tier_order\s*=\s*\[[^\]]+\]", src)
    assert matches, "tier_order assignment not found"

    # At least one should now use 'lurker' (the DB value)
    assert any("lurker" in m for m in matches), (
        "operator/app/routes/api.py tier_order must include 'lurker' to match the DB. "
        f"Found: {matches}"
    )

    # The /api/audience handler specifically must NOT use 'casual' anymore
    audience_section = src[src.find("def api_audience")
                           if "def api_audience" in src
                           else 0:]
    if "tier_order" in audience_section:
        idx = audience_section.find("tier_order")
        order_line = audience_section[idx: idx + 150]
        assert "'casual'" not in order_line and '"casual"' not in order_line, (
            f"/api/audience tier_order must use 'lurker' not 'casual'. Got: {order_line}"
        )


def test_frontend_tier_labels_include_lurker():
    """account-type.tsx tierLabels must include 'lurker' so the API value resolves to a label."""
    src = _read("lovable-frontend/src/lib/account-type.tsx")

    performer_match = re.search(
        r"PERFORMER_LABELS\s*:\s*Labels\s*=\s*\{(.*?)\n\};",
        src,
        re.DOTALL,
    )
    assert performer_match, "PERFORMER_LABELS not found"
    assert "lurker" in performer_match.group(1).lower(), (
        "PERFORMER_LABELS.tierLabels must include a 'lurker' key to resolve API values"
    )

    business_match = re.search(
        r"BUSINESS_LABELS\s*:\s*Labels\s*=\s*\{(.*?)\n\};",
        src,
        re.DOTALL,
    )
    assert business_match, "BUSINESS_LABELS not found"
    assert "lurker" in business_match.group(1).lower(), (
        "BUSINESS_LABELS.tierLabels must include a 'lurker' key"
    )


def test_frontend_tier_icons_include_lurker():
    """Components rendering tier icons must include 'lurker' in their TIER_ICONS dict."""
    for path in (
        "lovable-frontend/src/components/dashboard/FanTiers.tsx",
        "lovable-frontend/src/components/dashboard/FanOfTheWeek.tsx",
        "lovable-frontend/src/components/dashboard/FanHistoryTable.tsx",
    ):
        src = _read(path)
        match = re.search(r"TIER_ICONS[^=]*=\s*\{([^}]+)\}", src)
        assert match, f"TIER_ICONS not found in {path}"
        assert "lurker" in match.group(1).lower(), (
            f"{path} TIER_ICONS must include a 'lurker' key. Got:\n{match.group(0)}"
        )


# ---------------------------------------------------------------------------
# Billing in user menu
# ---------------------------------------------------------------------------

def test_user_menu_has_billing_link():
    """UserMenu must include a Billing menu item navigating to sp('/billing')."""
    src = _read("lovable-frontend/src/components/shell/UserMenu.tsx")

    assert re.search(r'navigate\s*\(\s*sp\s*\(\s*["\']\s*/billing\s*["\']\s*\)\s*\)', src), (
        "UserMenu must contain a DropdownMenuItem that navigates to sp('/billing')"
    )

    # Should mention 'Billing' user-facing label
    assert re.search(r"Billing", src), (
        "UserMenu must surface a 'Billing' label for the menu item"
    )


# ---------------------------------------------------------------------------
# Dashboard nav uses labels.audienceNav for business too
# ---------------------------------------------------------------------------

def test_dashboard_header_uses_audienceNav_label_for_business():
    """DashboardHeader business path must use labels.audienceNav, not hardcoded 'Audience'."""
    src = _read("lovable-frontend/src/components/shell/DashboardHeader.tsx")

    # Find the business branch — slice from the if-check to the closing return ].
    idx = src.find('if (accountType === "business")')
    assert idx != -1, "business branch in buildNavItems not found"

    # Extract until the closing `];` of the business return array
    snippet_start = src[idx:]
    end_idx = snippet_start.find("];")
    assert end_idx != -1, "business return array closing not found"
    body = snippet_start[: end_idx + 2]

    # Must reference labels.audienceNav
    assert "labels.audienceNav" in body, (
        "Business nav must use labels.audienceNav to render 'Customers' label, "
        f"not hardcode 'Audience'. Got:\n{body}"
    )

    # Must NOT have hardcoded `label: "Audience"` for the audience tab
    audience_line = re.search(r'label:\s*"Audience"[^}]*\{audience', body)
    assert not re.search(r'label:\s*"Audience"\s*,\s*to:\s*sp\("/audience"\)', body), (
        "Business nav must not hardcode the 'Audience' label (use labels.audienceNav)"
    )
