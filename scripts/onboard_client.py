#!/usr/bin/env python3
"""
onboard_client.py — One-command client onboarding.

Usage:
  # SMB client (fully automated Railway provisioning)
  python scripts/onboard_client.py --slug west_side_comedy

  # SMB with credentials inline (no prompts — good for CI)
  python scripts/onboard_client.py --slug my_barbershop \\
    --sms-number +18005551234 \\
    --owner-phone +19175559999 \\
    --portal-password supersecret

  # Creator / influencer
  python scripts/onboard_client.py --slug suzie

  # Dry-run (show what WOULD happen, touch nothing)
  python scripts/onboard_client.py --slug my_barbershop --dry-run

What it does automatically:
  ✓ Validates creator_config/<slug>.json exists and is well-formed
  ✓ Checks training_data/<slug>_chunks.json for creator types
  ✓ Resolves Railway service IDs by name (no hardcoding)
  ✓ Sets all required env vars on the web service
  ✓ For SMB: also sets vars on every cron service that needs them
  ✓ Prints a clear checklist of done vs still-manual steps
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    import requests as _requests
except ImportError:
    _requests = None  # fallback handled in _gql

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "creator_config"
TRAINING_DIR = ROOT / "training_data"

# ── ANSI colours ─────────────────────────────────────────────────────────────
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

def ok(msg):   print(f"  {_GREEN}✓{_RESET}  {msg}")
def warn(msg): print(f"  {_YELLOW}⚠{_RESET}  {msg}")
def err(msg):  print(f"  {_RED}✗{_RESET}  {msg}")
def info(msg): print(f"  {_CYAN}→{_RESET}  {msg}")
def hdr(msg):  print(f"\n{_BOLD}{msg}{_RESET}")


# ── Railway API helpers ───────────────────────────────────────────────────────

RAILWAY_API = "https://backboard.railway.app/graphql/v2"

def _gql(query: str, token: str) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"query": query}
    if _requests:
        resp = _requests.post(RAILWAY_API, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()
    # stdlib fallback (no SSL issues on Linux/CI)
    import urllib.request, urllib.error
    data = json.dumps(payload).encode()
    req = urllib.request.Request(RAILWAY_API, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Railway API HTTP {e.code}: {e.read().decode()}") from e


def get_railway_context() -> tuple[str, str, str]:
    """Return (api_token, project_id, environment_id) from env or abort."""
    token   = os.getenv("RAILWAY_API_TOKEN", "")
    project = os.getenv("RAILWAY_PROJECT_ID", "")
    env_id  = os.getenv("RAILWAY_ENVIRONMENT_ID", "")
    missing = [k for k, v in [
        ("RAILWAY_API_TOKEN", token),
        ("RAILWAY_PROJECT_ID", project),
        ("RAILWAY_ENVIRONMENT_ID", env_id),
    ] if not v]
    if missing:
        err(f"Missing env vars: {', '.join(missing)}")
        err("These are auto-set in Railway; run locally with a .env file.")
        sys.exit(1)
    return token, project, env_id


def list_services(token: str, project_id: str) -> dict[str, str]:
    """Return {service_name: service_id} for all services in the project."""
    result = _gql(
        f'{{ project(id: "{project_id}") {{ services {{ edges {{ node {{ id name }} }} }} }} }}',
        token,
    )
    edges = result["data"]["project"]["services"]["edges"]
    return {e["node"]["name"]: e["node"]["id"] for e in edges}


def upsert_var(token: str, project_id: str, env_id: str,
               service_id: str, name: str, value: str, dry_run: bool) -> None:
    if dry_run:
        info(f"[dry-run] would set  {name}=***  on service {service_id[:8]}…")
        return
    _gql(
        f'''mutation {{
          variableUpsert(input: {{
            projectId: "{project_id}"
            environmentId: "{env_id}"
            serviceId: "{service_id}"
            name: "{name}"
            value: {json.dumps(value)}
          }})
        }}''',
        token,
    )


def set_vars_on_service(token, project_id, env_id, service_id, service_name,
                        variables: dict[str, str], dry_run: bool) -> None:
    for name, value in variables.items():
        upsert_var(token, project_id, env_id, service_id, name, value, dry_run)
        action = "[dry-run]" if dry_run else "set"
        ok(f"{action}  {name}  →  {service_name}")


# ── Config validation ─────────────────────────────────────────────────────────

def load_and_validate_config(slug: str) -> dict:
    path = CONFIG_DIR / f"{slug}.json"
    if not path.exists():
        err(f"Config not found: {path}")
        err(f"Create creator_config/{slug}.json first. See creator_config/TEMPLATE.json or smb_template.json.")
        sys.exit(1)

    with open(path) as f:
        cfg = json.load(f)

    if cfg.get("slug") and cfg["slug"] != slug:
        warn(f"Config 'slug' field ({cfg['slug']!r}) doesn't match filename slug ({slug!r})")

    return cfg


def is_smb(cfg: dict) -> bool:
    """True if this is an SMB tenant (has business_type field, not a creator config)."""
    bt = cfg.get("business_type", "")
    # Creator templates have generic placeholder text; SMB ones have real types
    return bool(bt) and "e.g." not in bt


# ── SMB onboarding ────────────────────────────────────────────────────────────

# Services that need per-tenant SMS vars (besides 'web')
_CRON_SERVICES_NEEDING_SMB_VARS = {
    "SMB Quality Digest",
    "SMB Notion Integration",
}

def onboard_smb(cfg: dict, slug: str, args, dry_run: bool) -> None:
    hdr(f"SMB Client Onboarding — {cfg.get('display_name', slug)}")

    # ── 1. Validate config fields ──
    hdr("1 / 4  Validating config")
    required = ["display_name", "business_type", "tone", "welcome_message"]
    for field in required:
        if cfg.get(field):
            ok(field)
        else:
            warn(f"{field} is empty — fill this in creator_config/{slug}.json")

    if cfg.get("tracked_links"):
        ok(f"tracked_links ({len(cfg['tracked_links'])} links)")
    else:
        warn("tracked_links is empty — subscribers won't get trackable URLs")

    if cfg.get("signup_nudge"):
        ok("signup_nudge (custom copy set)")
    else:
        warn("signup_nudge not set — will use generic fallback copy")

    # ── 2. Collect credentials ──
    hdr("2 / 4  Collecting credentials")

    slug_upper = slug.upper()

    sms_number  = args.sms_number  or _prompt(f"Twilio SMS number for {slug} (e.g. +18005551234): ")
    owner_phone = args.owner_phone or _prompt(f"Owner's cell phone for {slug} (e.g. +19175559999): ")
    password    = args.portal_password or _prompt(f"Portal password for {slug} (shown at /portal/{slug}/login): ", secret=True)

    if not sms_number or not owner_phone or not password:
        err("SMS number, owner phone, and portal password are all required.")
        sys.exit(1)

    ok(f"SMS number    : {sms_number}")
    ok(f"Owner phone   : {owner_phone[:3]}…{owner_phone[-4:]}")
    ok(f"Portal password: {'*' * len(password)}")

    # ── 3. Set Railway vars ──
    hdr("3 / 4  Provisioning Railway variables")

    token, project_id, env_id = get_railway_context()
    services = list_services(token, project_id)

    smb_vars = {
        f"SMB_{slug_upper}_SMS_NUMBER":          sms_number,
        f"SMB_{slug_upper}_OWNER_PHONE":         owner_phone,
        f"SMB_PORTAL_{slug_upper}_PASSWORD":     password,
    }

    # Always set on 'web'
    web_id = services.get("web")
    if not web_id:
        err("Could not find 'web' service in Railway project.")
        sys.exit(1)
    set_vars_on_service(token, project_id, env_id, web_id, "web", smb_vars, dry_run)

    # Set on cron services that need tenant vars
    for svc_name in _CRON_SERVICES_NEEDING_SMB_VARS:
        svc_id = services.get(svc_name)
        if svc_id:
            set_vars_on_service(token, project_id, env_id, svc_id, svc_name, smb_vars, dry_run)
        else:
            warn(f"Service '{svc_name}' not found — skipping (may not exist yet)")

    # ── 4. Checklist ──
    hdr("4 / 4  What's left (manual steps)")
    print()
    print(f"  {'Done automatically':40s}  {'Remaining manual steps'}")
    print(f"  {'─' * 40}  {'─' * 40}")
    print(f"  {'✓ creator_config/' + slug + '.json validated':40s}  ⚠ Buy/provision Twilio number {sms_number}")
    print(f"  {'✓ Railway vars set on web + cron services':40s}  ⚠ Point Twilio number → /smb/inbound webhook")
    print(f"  {'✓ Portal available at /portal/' + slug + '/login':40s}  ⚠ Send welcome blast to first subscribers")
    print()
    if not dry_run:
        ok(f"{cfg.get('display_name', slug)} is ready — deploy web to activate.")
    else:
        info("Dry-run complete — no changes made.")


# ── Creator onboarding ────────────────────────────────────────────────────────

def onboard_creator(cfg: dict, slug: str, dry_run: bool) -> None:
    hdr(f"Creator Onboarding — {cfg.get('display_name') or cfg.get('name', slug)}")

    # ── 1. Validate config ──
    hdr("1 / 3  Validating config")
    for field in ["name", "voice_style", "hard_fact_guardrails_text",
                  "voice_lock_rules_text", "style_rules_text", "tone_examples_text"]:
        if cfg.get(field):
            ok(field)
        else:
            warn(f"{field} missing — AI will fall back to Zarna defaults for this field")

    links = cfg.get("links", {})
    for lk in ["tickets", "merch", "book", "youtube"]:
        if links.get(lk):
            ok(f"links.{lk}")
        else:
            warn(f"links.{lk} not set — AI will use Zarna's link as fallback")

    # ── 2. Validate training data ──
    hdr("2 / 3  Checking training data")
    chunks_path  = TRAINING_DIR / f"{slug}_chunks.json"
    embeddings_path = TRAINING_DIR / f"{slug}_embeddings.json.gz"

    if chunks_path.exists():
        with open(chunks_path) as f:
            chunks = json.load(f)
        ok(f"training_data/{slug}_chunks.json  ({len(chunks)} chunks)")
    else:
        err(f"training_data/{slug}_chunks.json not found")
        err("Run the ingestion pipeline on their content before deploying.")

    if embeddings_path.exists():
        ok(f"training_data/{slug}_embeddings.json.gz")
    else:
        warn(f"training_data/{slug}_embeddings.json.gz not found — retrieval will be degraded")

    # ── 3. Checklist ──
    hdr("3 / 3  What's left")
    print()
    print(f"  Done automatically")
    print(f"  ✓ creator_config/{slug}.json validated")
    print()
    print(f"  Remaining manual steps")
    print(f"  ⚠ Set CREATOR_SLUG={slug} in Railway (or deploy a separate service)")
    print(f"  ⚠ Deploy web service to activate new creator config")
    print()
    if not dry_run:
        info(f"Config for {slug} is validated. Set CREATOR_SLUG={slug} in Railway to activate.")
    else:
        info("Dry-run complete — no changes made.")


# ── Prompt helper ─────────────────────────────────────────────────────────────

def _prompt(msg: str, secret: bool = False) -> str:
    try:
        if secret:
            import getpass
            return getpass.getpass(f"  {_CYAN}?{_RESET}  {msg}").strip()
        return input(f"  {_CYAN}?{_RESET}  {msg}").strip()
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(0)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Onboard a new SMB client or creator — auto-provisions Railway variables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--slug", required=True,
                        help="Config slug (filename without .json, e.g. west_side_comedy)")
    parser.add_argument("--sms-number",     default="",
                        help="SMB only: Twilio SMS number (e.g. +18005551234)")
    parser.add_argument("--owner-phone",    default="",
                        help="SMB only: owner's cell phone (e.g. +19175559999)")
    parser.add_argument("--portal-password", default="",
                        help="SMB only: portal login password")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without making any changes")
    args = parser.parse_args()

    slug = args.slug.lower().replace("-", "_")
    dry_run = args.dry_run

    if dry_run:
        print(f"\n{_YELLOW}{_BOLD}DRY RUN — no changes will be made{_RESET}\n")

    # Load and detect type
    cfg = load_and_validate_config(slug)

    if is_smb(cfg):
        onboard_smb(cfg, slug, args, dry_run)
    else:
        onboard_creator(cfg, slug, dry_run)


if __name__ == "__main__":
    main()
