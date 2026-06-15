"""
Tests for the performer vCard + first-message sequence (Item 2).

The live handler wiring (main.py) is exercised indirectly: these tests cover the
building blocks — vCard construction, footer handling, and the opt-in send
sequence — using a fake messaging adapter so no Twilio/network calls happen.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tests.gemini_test_util import ensure_placeholder_key_for_import

ensure_placeholder_key_for_import()

import pytest

from app.brain.creator_config import CreatorConfig
from app.messaging import contact_card as cc


class FakeAdapter:
    def __init__(self, raise_on=None, fail_on=None):
        self.calls = []
        self.raise_on = raise_on  # 'mms' | 'text' | None — raises an exception
        self.fail_on = fail_on    # 'mms' | 'text' | None — returns False (send rejected)

    def send_reply(self, to_number, body="", from_number=None, media_url=None):
        kind = "mms" if media_url else "text"
        if self.raise_on == kind:
            raise RuntimeError("boom")
        self.calls.append({"to": to_number, "body": body, "from": from_number, "media_url": media_url})
        return self.fail_on != kind


class FakeStorage:
    def __init__(self, raise_on_save=False):
        self.saved = []
        self.raise_on_save = raise_on_save

    def save_message(self, phone_number, role, text):
        if self.raise_on_save:
            raise RuntimeError("db down")
        self.saved.append({"phone": phone_number, "role": role, "text": text})


def _cfg(**kw):
    base = dict(slug="perf", name="Performer Name")
    base.update(kw)
    return CreatorConfig(**base)


@pytest.fixture(autouse=True)
def _clear():
    cc.clear_photo_cache()
    yield
    cc.clear_photo_cache()


# ---------------------------------------------------------------------------
# vCard construction
# ---------------------------------------------------------------------------

class TestBuildVcard:
    def test_basic_structure_and_display_name(self):
        vcf = cc.build_performer_vcard(_cfg(sms_display_name="Zarna"))
        assert vcf.startswith("BEGIN:VCARD")
        assert vcf.strip().endswith("END:VCARD")
        assert "FN:Zarna" in vcf

    def test_falls_back_to_name_when_no_sms_display_name(self):
        assert "FN:Performer Name" in cc.build_performer_vcard(_cfg())

    def test_includes_tel_when_provided(self):
        vcf = cc.build_performer_vcard(_cfg(), tel="+12125551234")
        assert "TEL;TYPE=CELL:+12125551234" in vcf

    def test_no_tel_when_blank(self):
        assert "TEL" not in cc.build_performer_vcard(_cfg(), tel="")

    def test_embeds_photo_when_available(self, monkeypatch):
        monkeypatch.setattr(cc, "_load_photo_b64", lambda slug, url: ("image/jpeg", "QUJD"))
        vcf = cc.build_performer_vcard(_cfg(profile_photo_url="https://img/x.png"))
        # vCard 3.0 inline form so iOS renders the photo (not ENCODING=BASE64).
        assert "PHOTO;ENCODING=b;TYPE=JPEG:QUJD" in vcf

    def test_no_photo_line_when_no_url(self):
        assert "PHOTO" not in cc.build_performer_vcard(_cfg())

    def test_files_as_person_not_company(self):
        # Structured N (First/Last) + no ORG so iOS treats it as a person, not a
        # business — regression for the empty contact photo + business section.
        vcf = cc.build_performer_vcard(_cfg(sms_display_name="Zarna AI"))
        assert "N:AI;Zarna;;;" in vcf
        assert "FN:Zarna AI" in vcf
        assert "ORG:" not in vcf

    def test_single_word_name_has_empty_family(self):
        vcf = cc.build_performer_vcard(_cfg(sms_display_name="Zarna"))
        assert "N:;Zarna;;;" in vcf
        assert "FN:Zarna" in vcf

    def test_long_photo_base64_is_line_folded(self, monkeypatch):
        # Unfolded base64 made iOS drop the inline image; lines must be <=75 chars
        # with continuation lines starting with a single space (RFC 2426).
        monkeypatch.setattr(cc, "_load_photo_b64", lambda slug, url: ("image/jpeg", "A" * 500))
        vcf = cc.build_performer_vcard(_cfg(profile_photo_url="https://img/x.png"))
        photo_idx = [i for i, l in enumerate(vcf.split("\r\n")) if l.startswith("PHOTO")]
        assert photo_idx, "expected a PHOTO line"
        physical = vcf.split("\r\n")
        start = photo_idx[0]
        # First PHOTO line and its continuations all stay within 75 octets.
        assert len(physical[start]) <= 75
        assert physical[start + 1].startswith(" ")
        assert all(len(l) <= 75 for l in physical[start:start + 3])


# ---------------------------------------------------------------------------
# Photo loading — http(s) URLs and bundled local-file assets
# ---------------------------------------------------------------------------

class TestPhotoLoading:
    def test_reads_local_absolute_path(self, tmp_path):
        f = tmp_path / "pic.bin"
        f.write_bytes(b"rawbytes")
        assert cc._read_photo_bytes("perf", str(f)) == b"rawbytes"

    def test_resolves_relative_path_against_project_root(self, monkeypatch, tmp_path):
        monkeypatch.setattr(cc, "_PROJECT_ROOT", str(tmp_path))
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "pic.bin").write_bytes(b"hello")
        assert cc._read_photo_bytes("perf", "sub/pic.bin") == b"hello"

    def test_missing_local_file_returns_none(self):
        assert cc._read_photo_bytes("perf", "does/not/exist.png") is None

    def test_bundled_zarna_asset_loads_and_embeds(self):
        # Guards that the committed creator photo stays valid + decodable.
        pytest.importorskip("PIL")
        mime, b64 = cc._load_photo_b64("zarna", "app/assets/creator_photos/zarna.png")
        assert mime == "image/jpeg"
        assert len(b64) > 1000


# ---------------------------------------------------------------------------
# Footer handling
# ---------------------------------------------------------------------------

class TestFooter:
    def test_appends_footer(self):
        out = cc.first_message_with_footer("Hi, it's me!")
        assert out.startswith("Hi, it's me!")
        assert cc.COMPLIANCE_FOOTER in out

    def test_empty_in_empty_out(self):
        assert cc.first_message_with_footer("") == ""
        assert cc.first_message_with_footer("   ") == ""

    def test_does_not_double_append(self):
        once = cc.first_message_with_footer("Hi")
        assert cc.first_message_with_footer(once) == once


# ---------------------------------------------------------------------------
# Send sequence
# ---------------------------------------------------------------------------

class TestMaybeSendFirstContact:
    def test_noop_when_nothing_configured(self):
        a = FakeAdapter()
        assert cc.maybe_send_first_contact(a, "+1555", _cfg()) is False
        assert a.calls == []

    def test_noop_when_config_none(self):
        a = FakeAdapter()
        assert cc.maybe_send_first_contact(a, "+1555", None) is False

    def test_sends_welcome_only(self):
        a = FakeAdapter()
        sent = cc.maybe_send_first_contact(a, "+1555", _cfg(first_message="Welcome!"))
        assert sent is True
        assert len(a.calls) == 1
        assert a.calls[0]["media_url"] is None
        assert "Welcome!" in a.calls[0]["body"]
        assert cc.COMPLIANCE_FOOTER in a.calls[0]["body"]

    def test_sends_vcard_then_welcome(self, monkeypatch):
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        # Regression guard: the operator API domain must NOT be used for the
        # vCard media URL (it 404s -> Twilio 11200).
        monkeypatch.setenv("OPERATOR_API_BASE_URL", "https://operator.wrong")
        a = FakeAdapter()
        cfg = _cfg(send_contact_card=True, first_message="Hey!", sms_display_name="Z")
        sent = cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999")
        assert sent is True
        assert len(a.calls) == 2
        # vCard MMS first, with the media URL pointing at the performer route.
        assert a.calls[0]["media_url"].startswith("https://api.test/vcard/performer/perf.vcf")
        assert "operator.wrong" not in a.calls[0]["media_url"]
        assert "tel=%2B1999" in a.calls[0]["media_url"]
        assert a.calls[0]["from"] == "+1999"
        # Welcome text second.
        assert a.calls[1]["media_url"] is None
        assert "Hey!" in a.calls[1]["body"]

    def test_vcard_skipped_without_base_url_but_welcome_sent(self, monkeypatch):
        monkeypatch.delenv("PERFORMER_VCARD_BASE_URL", raising=False)
        monkeypatch.delenv("OPERATOR_API_BASE_URL", raising=False)
        monkeypatch.delenv("RAILWAY_PUBLIC_DOMAIN", raising=False)
        monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
        a = FakeAdapter()
        cfg = _cfg(send_contact_card=True, first_message="Hey!")
        sent = cc.maybe_send_first_contact(a, "+1555", cfg)
        # Only the welcome text goes out (no base URL to host the vCard).
        assert sent is True
        assert len(a.calls) == 1
        assert a.calls[0]["media_url"] is None

    def test_card_only_no_first_message(self, monkeypatch):
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter()
        cfg = _cfg(send_contact_card=True)
        sent = cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999")
        assert sent is True
        assert len(a.calls) == 1
        assert a.calls[0]["media_url"] is not None

    def test_never_raises_when_adapter_fails(self, monkeypatch):
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter(raise_on="mms")
        cfg = _cfg(send_contact_card=True, first_message="Hey!")
        # MMS raises, but the welcome text still sends and no exception escapes.
        sent = cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999")
        assert sent is True
        assert any(c["media_url"] is None for c in a.calls)

    def test_persists_card_and_welcome_to_inbox(self, monkeypatch):
        # When storage is provided, both the vCard MMS and the welcome text are
        # recorded as assistant turns so they appear in the operator inbox.
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter()
        s = FakeStorage()
        cfg = _cfg(send_contact_card=True, first_message="Hey!", sms_display_name="Z")
        cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999", storage=s)
        assert [m["role"] for m in s.saved] == ["assistant", "assistant"]
        assert all(m["phone"] == "+1555" for m in s.saved)
        assert "Tap to save Z to your contacts." in s.saved[0]["text"]
        assert "Hey!" in s.saved[1]["text"]

    def test_no_storage_does_not_persist_or_error(self, monkeypatch):
        # Backward-compatible: omitting storage keeps the old send-only behaviour.
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter()
        cfg = _cfg(send_contact_card=True, first_message="Hey!")
        assert cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999") is True

    def test_does_not_persist_when_send_rejected(self, monkeypatch):
        # If Twilio rejects the MMS (send_reply returns False) we must not record
        # a card the fan never received; the welcome still sends and is recorded.
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter(fail_on="mms")
        s = FakeStorage()
        cfg = _cfg(send_contact_card=True, first_message="Hey!", sms_display_name="Z")
        cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999", storage=s)
        assert [m["text"] for m in s.saved] == [
            t for t in (cc.first_message_with_footer("Hey!"),)
        ]

    def test_persist_failure_never_breaks_send(self, monkeypatch):
        # A storage outage must not stop the messages going out or raise.
        monkeypatch.setenv("PUBLIC_BASE_URL", "https://api.test")
        a = FakeAdapter()
        s = FakeStorage(raise_on_save=True)
        cfg = _cfg(send_contact_card=True, first_message="Hey!", sms_display_name="Z")
        sent = cc.maybe_send_first_contact(a, "+1555", cfg, from_number="+1999", storage=s)
        assert sent is True
        assert len(a.calls) == 2  # both messages still sent
        assert s.saved == []      # nothing recorded, but no exception escaped

    def test_vcard_base_url_never_uses_operator_api(self, monkeypatch):
        # The vCard is served by THIS app; OPERATOR_API_BASE_URL points at a
        # different service and must never be used (regression for MMS 11200).
        monkeypatch.delenv("PERFORMER_VCARD_BASE_URL", raising=False)
        monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
        monkeypatch.delenv("RAILWAY_PUBLIC_DOMAIN", raising=False)
        monkeypatch.setenv("OPERATOR_API_BASE_URL", "https://api.zar.bot")
        assert cc._vcard_base_url() == ""
        # The app's own Railway domain is used instead.
        monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "web-prod.up.railway.app")
        assert cc._vcard_base_url() == "https://web-prod.up.railway.app"
        # An explicit override wins over everything.
        monkeypatch.setenv("PERFORMER_VCARD_BASE_URL", "https://cards.example.com")
        assert cc._vcard_base_url() == "https://cards.example.com"


# ---------------------------------------------------------------------------
# Dashboard overlay: the "My Bot" toggle (bot_configs) drives the live config
# ---------------------------------------------------------------------------

class TestDashboardOverlay:
    def test_overrides_applied_on_top_of_file(self, monkeypatch):
        import app.brain.creator_config as ccfg
        monkeypatch.setattr(ccfg, "_load_bot_overrides", lambda slug: {
            "send_contact_card": True,
            "sms_display_name": "Zarna G",
            "first_message": "Hey, it's Zarna!",
            "profile_photo_url": "https://img.example/zarna.jpg",
        })
        cfg = ccfg.load_creator("zarna")
        assert cfg is not None
        assert cfg.send_contact_card is True
        assert cfg.sms_display_name == "Zarna G"
        assert cfg.first_message == "Hey, it's Zarna!"
        assert cfg.profile_photo_url == "https://img.example/zarna.jpg"

    def test_off_by_default_when_no_overrides(self, monkeypatch):
        import app.brain.creator_config as ccfg
        monkeypatch.setattr(ccfg, "_load_bot_overrides", lambda slug: {})
        cfg = ccfg.load_creator("zarna")
        assert cfg is not None
        # Zarna's file sets none of these — feature stays OFF until toggled.
        assert cfg.send_contact_card is False
        assert cfg.first_message == ""
        assert cfg.profile_photo_url == ""

    def test_overlay_only_touches_allowlisted_fields(self):
        import app.brain.creator_config as ccfg
        assert set(ccfg._BOT_OVERRIDE_FIELDS) == {
            "send_contact_card", "profile_photo_url", "sms_display_name", "first_message",
        }
