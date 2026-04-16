"""
SMB Tenant Registry

Loads all SMB business configs from creator_config/*.json at startup.
Provides routing helpers to map inbound Twilio messages to the correct
business tenant based on the destination (To) phone number, and to
identify whether a sender is the registered business owner.

Discriminator: any creator_config JSON with a "business_type" field is
treated as an SMB tenant. Performer configs (zarna.json, TEMPLATE.json)
do not have this field and are ignored automatically.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent.parent.parent / "creator_config"


@dataclass
class BusinessTenant:
    slug: str
    display_name: str
    business_type: str
    sms_number: Optional[str]       # Twilio number subscribers text in to
    owner_phone: Optional[str]      # Owner's cell — messages from here are blast commands
    keyword: Optional[str]          # Signup keyword (e.g. "LAUGH")
    tone: str = ""
    welcome_message: str = ""
    outreach_invite_message: str = ""  # default message for send_outreach_invites.py blasts
    signup_question: str = ""          # single open-ended onboarding question
    signup_questions: list = field(default_factory=list)  # legacy multi-step (fallback)
    value_content_topics: list = field(default_factory=list)
    blast_triggers: list = field(default_factory=list)
    segments: list = field(default_factory=list)
    logo_url: str = ""
    timezone: str = "America/New_York"   # IANA tz used for date labels (Tonight, Tomorrow, etc.)
    # Tracked short-links served at /smb/r/<slug>/<link_key>
    # Keyed by link_key (e.g. "tickets", "calendar") → destination URL.
    # If empty, the redirect endpoint returns 404 for this tenant.
    tracked_links: dict = field(default_factory=dict)
    # Optional override copy — neutral defaults are used when these are empty.
    signup_nudge: str = ""         # SMS sent to unknown subscribers ("Reply YES to join")
    checkin_confirmation: str = "" # Reply sent on a successful show check-in
    raw: dict = field(default_factory=dict)


def _env_prefix(slug: str) -> str:
    return "SMB_" + slug.upper()


def _load_tenant(path: Path) -> Optional[BusinessTenant]:
    try:
        with open(path) as f:
            cfg = json.load(f)
    except Exception:
        logger.exception("SMB tenants: failed to read %s", path)
        return None

    if "business_type" not in cfg:
        return None  # Performer config or template — not an SMB tenant

    slug = cfg.get("slug", "")
    if not slug or slug == "your_business_slug":
        return None  # smb_template.json placeholder — skip

    prefix = _env_prefix(slug)

    def _resolve(env_key: str, cfg_key: str) -> Optional[str]:
        """Prefer env var; fall back to config value unless it's a TBD placeholder."""
        val = os.getenv(env_key, "").strip()
        if val:
            return val
        cfg_val = cfg.get(cfg_key, "TBD")
        return None if cfg_val in ("TBD", "", None) else cfg_val

    return BusinessTenant(
        slug=slug,
        display_name=cfg.get("display_name", slug),
        business_type=cfg.get("business_type", ""),
        sms_number=_resolve(f"{prefix}_SMS_NUMBER", "sms_number"),
        owner_phone=_resolve(f"{prefix}_OWNER_PHONE", "owner_phone"),
        keyword=_resolve(f"{prefix}_KEYWORD", "sms_keyword"),
        tone=cfg.get("tone", ""),
        welcome_message=cfg.get("welcome_message", ""),
        outreach_invite_message=cfg.get("outreach_invite_message", ""),
        signup_question=cfg.get("signup_question", ""),
        signup_questions=cfg.get("signup_questions", []),
        value_content_topics=cfg.get("value_content_topics", []),
        blast_triggers=cfg.get("blast_triggers", []),
        segments=cfg.get("segments", []),
        logo_url=cfg.get("logo_url", ""),
        timezone=cfg.get("timezone", "America/New_York"),
        tracked_links=cfg.get("tracked_links", {}),
        signup_nudge=cfg.get("signup_nudge", ""),
        checkin_confirmation=cfg.get("checkin_confirmation", ""),
        raw=cfg,
    )


class TenantRegistry:
    def __init__(self):
        self._by_slug: dict[str, BusinessTenant] = {}
        self._by_sms_number: dict[str, BusinessTenant] = {}
        self._by_owner_phone: dict[str, BusinessTenant] = {}
        self._load()

    def _load(self) -> None:
        for path in sorted(_CONFIG_DIR.glob("*.json")):
            tenant = _load_tenant(path)
            if tenant is None:
                continue
            self._by_slug[tenant.slug] = tenant
            if tenant.sms_number:
                self._by_sms_number[tenant.sms_number] = tenant
            if tenant.owner_phone:
                self._by_owner_phone[tenant.owner_phone] = tenant
            logger.info(
                "SMB tenant loaded: %s (sms_number=%s owner=%s)",
                tenant.slug,
                tenant.sms_number or "TBD",
                "set" if tenant.owner_phone else "TBD",
            )
        logger.info("SMB TenantRegistry: %d tenant(s) loaded", len(self._by_slug))

    def get_by_slug(self, slug: str) -> Optional[BusinessTenant]:
        return self._by_slug.get(slug)

    def get_by_to_number(self, to_number: str) -> Optional[BusinessTenant]:
        """Return the tenant that owns this Twilio destination number, or None."""
        return self._by_sms_number.get(to_number)

    def is_owner(self, from_number: str, tenant: BusinessTenant) -> bool:
        """Return True if the sender is the registered owner of this tenant."""
        return bool(tenant.owner_phone and from_number == tenant.owner_phone)

    def all_tenants(self) -> list[BusinessTenant]:
        return list(self._by_slug.values())

    def is_smb_number(self, phone: str) -> bool:
        """Return True if *phone* is the inbound SMS number of any registered SMB tenant."""
        return phone in self._by_sms_number


# Module-level singleton — loaded once at startup, shared across all requests.
_registry: Optional[TenantRegistry] = None


def get_registry() -> TenantRegistry:
    global _registry
    if _registry is None:
        _registry = TenantRegistry()
    return _registry
