"""
Tests for app/smb/tenants.py

Covers:
- SMB configs are loaded; performer configs are skipped
- TBD placeholders resolve to None when no env var is set
- Env vars override TBD placeholders
- get_by_slug, get_by_to_number, is_owner routing helpers
- smb_template.json placeholder is skipped
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import importlib
import app.smb.tenants as tenants_module
from app.smb.tenants import TenantRegistry, _load_tenant
from pathlib import Path
import tempfile
import json


def _make_config(slug="test_biz", extra=None):
    cfg = {
        "slug": slug,
        "display_name": "Test Biz",
        "business_type": "barbershop",
        "owner_phone": "TBD",
        "sms_keyword": "TBD",
        "tone": "casual",
        "value_content_topics": ["haircare tips"],
        "signup_questions": ["Do you want deals?"],
        "blast_triggers": ["opening", "deal"],
    }
    if extra:
        cfg.update(extra)
    return cfg


def _write_config(directory, filename, cfg):
    path = Path(directory) / filename
    with open(path, "w") as f:
        json.dump(cfg, f)
    return path


# ---------------------------------------------------------------------------
# _load_tenant
# ---------------------------------------------------------------------------

def test_load_tenant_returns_none_for_performer_config():
    """Configs without business_type are not SMB tenants."""
    with tempfile.TemporaryDirectory() as d:
        path = _write_config(d, "zarna.json", {"slug": "zarna", "display_name": "Zarna Garg"})
        assert _load_tenant(path) is None
    print("✓ performer config (no business_type) correctly skipped")


def test_load_tenant_returns_none_for_template_placeholder():
    """smb_template.json placeholder slug is skipped."""
    with tempfile.TemporaryDirectory() as d:
        path = _write_config(d, "smb_template.json", _make_config(slug="your_business_slug"))
        assert _load_tenant(path) is None
    print("✓ template placeholder slug correctly skipped")


def test_load_tenant_tbd_resolves_to_none(monkeypatch):
    """TBD phone values with no env var set resolve to None."""
    monkeypatch.delenv("SMB_TEST_BIZ_OWNER_PHONE", raising=False)
    monkeypatch.delenv("SMB_TEST_BIZ_SMS_NUMBER", raising=False)
    monkeypatch.delenv("SMB_TEST_BIZ_KEYWORD", raising=False)
    with tempfile.TemporaryDirectory() as d:
        path = _write_config(d, "test_biz.json", _make_config())
        tenant = _load_tenant(path)
    assert tenant is not None
    assert tenant.owner_phone is None
    assert tenant.sms_number is None
    assert tenant.keyword is None
    print("✓ TBD placeholders resolve to None when env vars absent")


def test_load_tenant_env_var_overrides_tbd(monkeypatch):
    """Env vars are picked up even when config says TBD."""
    monkeypatch.setenv("SMB_TEST_BIZ_OWNER_PHONE", "+15550001111")
    monkeypatch.setenv("SMB_TEST_BIZ_SMS_NUMBER", "+15550002222")
    monkeypatch.setenv("SMB_TEST_BIZ_KEYWORD", "CUTS")
    with tempfile.TemporaryDirectory() as d:
        path = _write_config(d, "test_biz.json", _make_config())
        tenant = _load_tenant(path)
    assert tenant.owner_phone == "+15550001111"
    assert tenant.sms_number == "+15550002222"
    assert tenant.keyword == "CUTS"
    print("✓ env vars override TBD placeholders correctly")


def test_load_tenant_fields(monkeypatch):
    """Loaded tenant has correct metadata from config."""
    monkeypatch.delenv("SMB_TEST_BIZ_OWNER_PHONE", raising=False)
    monkeypatch.delenv("SMB_TEST_BIZ_SMS_NUMBER", raising=False)
    monkeypatch.delenv("SMB_TEST_BIZ_KEYWORD", raising=False)
    with tempfile.TemporaryDirectory() as d:
        path = _write_config(d, "test_biz.json", _make_config())
        tenant = _load_tenant(path)
    assert tenant.slug == "test_biz"
    assert tenant.display_name == "Test Biz"
    assert tenant.business_type == "barbershop"
    assert tenant.tone == "casual"
    assert "haircare tips" in tenant.value_content_topics
    assert "opening" in tenant.blast_triggers
    print("✓ tenant fields loaded correctly from config")


# ---------------------------------------------------------------------------
# TenantRegistry routing helpers
# ---------------------------------------------------------------------------

def _make_registry_with_tenant(tmp_dir, owner_phone="+15550001111", sms_number="+15550002222"):
    """Build a TenantRegistry pointed at a temp config directory."""
    cfg = _make_config(extra={"owner_phone": owner_phone, "sms_keyword": "CUTS"})
    _write_config(tmp_dir, "test_biz.json", cfg)
    # Patch env so sms_number resolves
    os.environ["SMB_TEST_BIZ_SMS_NUMBER"] = sms_number
    os.environ["SMB_TEST_BIZ_OWNER_PHONE"] = owner_phone

    # Temporarily redirect _CONFIG_DIR to our temp dir
    original = tenants_module._CONFIG_DIR
    tenants_module._CONFIG_DIR = Path(tmp_dir)
    registry = TenantRegistry()
    tenants_module._CONFIG_DIR = original
    os.environ.pop("SMB_TEST_BIZ_SMS_NUMBER", None)
    os.environ.pop("SMB_TEST_BIZ_OWNER_PHONE", None)
    return registry


def test_get_by_slug():
    with tempfile.TemporaryDirectory() as d:
        registry = _make_registry_with_tenant(d)
        tenant = registry.get_by_slug("test_biz")
        assert tenant is not None
        assert tenant.slug == "test_biz"
        assert registry.get_by_slug("nonexistent") is None
    print("✓ get_by_slug works")


def test_get_by_to_number():
    with tempfile.TemporaryDirectory() as d:
        registry = _make_registry_with_tenant(d, sms_number="+15550002222")
        assert registry.get_by_to_number("+15550002222") is not None
        assert registry.get_by_to_number("+19999999999") is None
    print("✓ get_by_to_number routes correctly")


def test_is_owner_true_and_false():
    with tempfile.TemporaryDirectory() as d:
        registry = _make_registry_with_tenant(d, owner_phone="+15550001111")
        tenant = registry.get_by_slug("test_biz")
        assert registry.is_owner("+15550001111", tenant) is True
        assert registry.is_owner("+19999999999", tenant) is False
    print("✓ is_owner correctly identifies owner vs. subscriber")


def test_real_west_side_comedy_config_loads():
    """West Side Comedy config in the actual repo loads without errors."""
    registry = TenantRegistry()
    tenant = registry.get_by_slug("west_side_comedy")
    assert tenant is not None
    assert tenant.display_name == "West Side Comedy Club"
    assert tenant.business_type == "comedy_club"
    assert len(tenant.value_content_topics) > 0
    assert len(tenant.blast_triggers) > 0
    # TBD fields are None, not the string "TBD"
    assert tenant.owner_phone is None
    assert tenant.sms_number is None
    print("✓ west_side_comedy config loads; TBD fields are None not strings")


def test_zarna_config_not_loaded_as_smb():
    """Zarna's performer config is NOT loaded as an SMB tenant."""
    registry = TenantRegistry()
    assert registry.get_by_slug("zarna") is None
    print("✓ zarna performer config correctly excluded from SMB registry")
