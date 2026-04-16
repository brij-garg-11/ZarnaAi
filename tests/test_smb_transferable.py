"""
Tests for SMB platform transferability (Blocker fixes).

Covers:
  1. Tracked links load from config, not hardcoded dict.
  2. Signup nudge uses tenant copy or neutral fallback — never leaks WSCC strings.
  3. Check-in confirmation uses tenant copy or neutral fallback.
  4. Operator portal routes are dynamic (no hardcoded west_side_comedy slug).
  5. Zero WSCC bleed-through for test_smb_client tenant.
"""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make app importable from project root
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Placeholder key so google.generativeai doesn't blow up at import time.
if not os.environ.get("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = "placeholder-for-import"


# ---------------------------------------------------------------------------
# Helper: build a BusinessTenant from a creator_config JSON file
# ---------------------------------------------------------------------------

def _load_tenant(slug: str):
    from app.smb.tenants import _load_tenant as load
    path = _ROOT / "creator_config" / f"{slug}.json"
    return load(path)


# ===========================================================================
# 1. Tracked links — loaded from config
# ===========================================================================

class TestTrackedLinks:

    def test_wscc_links_in_config(self):
        tenant = _load_tenant("west_side_comedy")
        assert tenant is not None
        assert "tickets" in tenant.tracked_links
        assert "calendar" in tenant.tracked_links
        assert "menu" in tenant.tracked_links
        assert "map" in tenant.tracked_links

    def test_wscc_links_point_to_correct_urls(self):
        tenant = _load_tenant("west_side_comedy")
        assert "westsidecomedyclub.com" in tenant.tracked_links["tickets"]
        assert "calendar" in tenant.tracked_links["calendar"]

    def test_test_client_has_its_own_links(self):
        tenant = _load_tenant("test_smb_client")
        assert tenant is not None
        assert "book" in tenant.tracked_links
        assert "deals" in tenant.tracked_links
        # Must NOT contain any WSCC URLs
        for url in tenant.tracked_links.values():
            assert "westsidecomedyclub" not in url, f"WSCC URL leaked into test_smb_client: {url}"

    def test_blueprint_redirect_uses_tenant_config(self):
        """smb_link_redirect looks up links from the tenant registry, not a hardcoded dict."""
        src = (_ROOT / "app" / "smb" / "blueprint.py").read_text()
        # The old hardcoded dict must not exist anywhere in the file
        assert "_TRACKED_LINKS" not in src, "blueprint.py still defines _TRACKED_LINKS"
        assert "tracked_links" in src, "blueprint.py must read from tenant.tracked_links"


# ===========================================================================
# 2. Signup nudge
# ===========================================================================

class TestSignupNudge:

    def _nudge(self, slug: str) -> str:
        from app.smb.brain import _signup_nudge
        tenant = _load_tenant(slug)
        return _signup_nudge(tenant)

    def test_wscc_nudge_uses_config_copy(self):
        nudge = self._nudge("west_side_comedy")
        assert "West Side Comedy Club" in nudge

    def test_test_client_nudge_uses_config_copy(self):
        nudge = self._nudge("test_smb_client")
        assert "Test Barbershop" in nudge
        # Must not leak WSCC strings
        assert "West Side Comedy" not in nudge
        assert "comedy club" not in nudge.lower()

    def test_neutral_fallback_does_not_mention_shows(self):
        """When signup_nudge is not set, fallback copy is generic (no 'show updates')."""
        from app.smb.brain import _signup_nudge
        from app.smb.tenants import BusinessTenant
        tenant = BusinessTenant(
            slug="anon",
            display_name="Acme Shop",
            business_type="shop",
            sms_number=None,
            owner_phone=None,
            keyword=None,
            signup_nudge="",  # empty — triggers fallback
        )
        nudge = _signup_nudge(tenant)
        assert "show update" not in nudge.lower(), f"Fallback nudge is entertainment-specific: {nudge}"
        assert "Acme Shop" in nudge


# ===========================================================================
# 3. Check-in confirmation
# ===========================================================================

class TestCheckinConfirmation:

    def _build_show(self):
        return {"id": 1, "name": "Friday Night Live", "checkin_keyword": "FRI"}

    def test_wscc_confirmation_uses_config_copy(self):
        """WSCC gets its custom confirmation that mentions 'show'."""
        tenant = _load_tenant("west_side_comedy")
        assert "enjoy the show" in tenant.checkin_confirmation.lower()

    def test_test_client_confirmation_is_its_own(self):
        tenant = _load_tenant("test_smb_client")
        assert "West Side" not in tenant.checkin_confirmation
        assert "comedy club" not in tenant.checkin_confirmation.lower()

    def test_neutral_fallback_says_event_not_show(self):
        """When checkin_confirmation is empty, fallback says 'enjoy the event'."""
        from app.smb.tenants import BusinessTenant
        from app.smb import brain as smb_brain
        from unittest.mock import patch, MagicMock

        tenant = BusinessTenant(
            slug="anon",
            display_name="Acme",
            business_type="shop",
            sms_number=None,
            owner_phone=None,
            keyword=None,
            checkin_confirmation="",
        )
        show = self._build_show()

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("app.smb.brain.get_db_connection", return_value=mock_conn), \
             patch("app.smb.storage.get_show_by_keyword", return_value=show), \
             patch("app.smb.storage.record_checkin", return_value=True):
            reply = smb_brain._try_show_checkin("+15551234567", show["checkin_keyword"], tenant)

        assert reply is not None
        assert "enjoy the event" in reply.lower(), f"Neutral fallback should say 'enjoy the event', got: {reply}"
        assert "enjoy the show" not in reply.lower(), f"Neutral fallback should not say 'enjoy the show', got: {reply}"


# ===========================================================================
# 4. Operator portal — dynamic slug routes
# ===========================================================================

class TestOperatorPortalSlugRoutes:
    """
    The operator/ directory conflicts with Python's built-in 'operator' module,
    so we inspect the source file directly rather than importing it.
    """

    _PORTAL_SRC = (_ROOT / "operator" / "app" / "routes" / "smb_portal.py").read_text()

    def test_no_hardcoded_slug_in_route_decorators(self):
        """Portal routes must not contain /portal/west_side_comedy/ in their path."""
        assert "/portal/west_side_comedy/" not in self._PORTAL_SRC, (
            "operator/app/routes/smb_portal.py still has hardcoded /portal/west_side_comedy/ routes"
        )

    def test_slug_placeholder_in_all_routes(self):
        """All portal routes must use <slug> in the URL pattern."""
        assert '"/portal/<slug>/login"' in self._PORTAL_SRC
        assert '"/portal/<slug>/logout"' in self._PORTAL_SRC
        assert '"/portal/<slug>/"' in self._PORTAL_SRC
        assert '"/portal/<slug>/shows"' in self._PORTAL_SRC
        assert '"/portal/<slug>/blast"' in self._PORTAL_SRC

    def test_session_key_is_slug_scoped(self):
        """_session_key() must be present and interpolate the slug."""
        assert "def _session_key(slug" in self._PORTAL_SRC
        assert "smb_portal_auth_{slug}" in self._PORTAL_SRC or "f\"smb_portal_auth_{slug}\"" in self._PORTAL_SRC

    def test_password_env_var_is_slug_scoped(self):
        """Password env-var lookup must be slug-dependent, not hardcoded."""
        assert "SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD" not in self._PORTAL_SRC, (
            "Hardcoded SMB_PORTAL_WEST_SIDE_COMEDY_PASSWORD env var still present"
        )
        # Dynamic derivation via slug.upper() must be present
        assert "slug.upper()" in self._PORTAL_SRC or "SLUG_UPPER" in self._PORTAL_SRC.upper()

    def test_hardcoded_display_name_gone(self):
        """The portal must not hardcode 'West Side Comedy Club' as a display name."""
        # It may appear in a comment or docstring, but NOT in HTML output or constants
        lines = self._PORTAL_SRC.splitlines()
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue
            if "_DISPLAY_NAME" in line or ("West Side Comedy Club" in line and "=" in line and "NOTE" not in line and "note" not in line):
                assert False, f"Hardcoded display name still present: {line.strip()}"


# ===========================================================================
# 5. Zero bleed-through: test_smb_client never sees WSCC content
# ===========================================================================

class TestSMBBleedThrough:

    _WSCC_STRINGS = [
        "west side comedy",
        "westsidecomedyclub",
        "felicia",
        "West Side Comedy Club",
        "WSCC",
        "Upper West Side",
        "West 75th",
        "201 west",
    ]

    def _tenant(self, slug: str):
        return _load_tenant(slug)

    def test_tracked_links_no_wscc_urls(self):
        tenant = self._tenant("test_smb_client")
        for key, url in tenant.tracked_links.items():
            for s in self._WSCC_STRINGS:
                assert s.lower() not in url.lower(), (
                    f"WSCC string '{s}' leaked into test_smb_client tracked_links[{key}]={url}"
                )

    def test_signup_nudge_no_wscc(self):
        from app.smb.brain import _signup_nudge
        tenant = self._tenant("test_smb_client")
        nudge = _signup_nudge(tenant)
        for s in self._WSCC_STRINGS:
            assert s.lower() not in nudge.lower(), (
                f"WSCC string '{s}' leaked into test_smb_client signup nudge: {nudge}"
            )

    def test_checkin_confirmation_no_wscc(self):
        tenant = self._tenant("test_smb_client")
        conf = tenant.checkin_confirmation
        for s in self._WSCC_STRINGS:
            assert s.lower() not in conf.lower(), (
                f"WSCC string '{s}' leaked into test_smb_client checkin_confirmation: {conf}"
            )

    def test_welcome_message_no_wscc(self):
        tenant = self._tenant("test_smb_client")
        msg = tenant.welcome_message
        for s in self._WSCC_STRINGS:
            assert s.lower() not in msg.lower(), (
                f"WSCC string '{s}' leaked into test_smb_client welcome_message: {msg}"
            )

    def test_config_raw_no_wscc_bleed(self):
        """The JSON itself must not accidentally contain WSCC content."""
        path = _ROOT / "creator_config" / "test_smb_client.json"
        raw = path.read_text()
        for s in ["westsidecomedyclub", "West Side Comedy Club", "Felicia", "West 75th"]:
            assert s not in raw, f"'{s}' found in test_smb_client.json"
