"""
SlickText v2 — Campaign API for Live Show broadcasts.

Flow (one shot from SlickText's perspective after we sync):
1. Create a **temporary List** on the brand (so this blast doesn't mix with others).
2. For each phone: **find or create** a Contact (`GET ?mobile_number=` then `POST`).
3. **POST** `/brands/{id}/lists/contacts` with `{contact_id, lists:[list_id]}` batches.
4. **POST** `/brands/{id}/campaigns/` with `status: "send"` and
   `audience: {"contact_lists": [list_id]}`.

Requires **v2** credentials (`SLICKTEXT_API_KEY` + `SLICKTEXT_BRAND_ID`).
Legacy v1-only accounts cannot use this path — use per-number loop mode instead.

Docs: https://api.slicktext.com/docs/v2/campaigns
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, List, Optional

import requests

logger = logging.getLogger(__name__)

_V2_BASE = "https://dev.slicktext.com/v1"


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


@dataclass
class SlickTextCampaignResult:
    ok: bool
    campaign_id: Optional[int]
    list_id: Optional[int]
    contacts_synced: int
    contacts_failed: int
    error: Optional[str]


def _normalize_mobile_key(m: str) -> str:
    return "".join(c for c in m if c.isdigit() or c == "+")


def _get_contact_by_mobile(api_key: str, brand_id: str, mobile: str) -> Optional[int]:
    """Return contact_id if SlickText returns a row for this mobile (first page)."""
    url = f"{_V2_BASE}/brands/{brand_id}/contacts"
    want = _normalize_mobile_key(mobile)
    try:
        r = requests.get(
            url,
            headers=_headers(api_key),
            params={"mobile_number": mobile},
            timeout=30,
        )
        if r.status_code != 200:
            logger.debug("SlickText contact lookup %s: %s %s", mobile[-4:], r.status_code, r.text[:200])
            return None
        data = r.json()
        rows = data.get("data") or []
        for row in rows:
            got = _normalize_mobile_key(str(row.get("mobile_number") or ""))
            if got == want or got.endswith(want.lstrip("+")) or want.endswith(got.lstrip("+")):
                cid = row.get("contact_id")
                return int(cid) if cid is not None else None
        return None
    except requests.RequestException as e:
        logger.warning("SlickText contact lookup error: %s", e)
        return None


def _create_contact(api_key: str, brand_id: str, mobile: str) -> Optional[int]:
    url = f"{_V2_BASE}/brands/{brand_id}/contacts"
    payload = {
        "mobile_number": mobile,
        "opt_in_status": "subscribed",
        "language": "en",
    }
    try:
        r = requests.post(url, headers=_headers(api_key), json=payload, timeout=30)
        if r.status_code == 200:
            data = r.json()
            cid = data.get("contact_id")
            return int(cid) if cid is not None else None
        logger.warning("SlickText create contact %s: %s %s", mobile[-4:], r.status_code, r.text[:300])
        return None
    except requests.RequestException as e:
        logger.warning("SlickText create contact error: %s", e)
        return None


def _ensure_contact_id(api_key: str, brand_id: str, mobile: str) -> Optional[int]:
    cid = _get_contact_by_mobile(api_key, brand_id, mobile)
    if cid:
        return cid
    return _create_contact(api_key, brand_id, mobile)


def _create_list(api_key: str, brand_id: str, name: str, description: str) -> Optional[int]:
    url = f"{_V2_BASE}/brands/{brand_id}/lists"
    try:
        r = requests.post(
            url,
            headers=_headers(api_key),
            json={"name": name[:120], "description": description[:500]},
            timeout=30,
        )
        if r.status_code == 200:
            data = r.json()
            lid = data.get("contact_list_id")
            return int(lid) if lid is not None else None
        logger.error("SlickText create list failed: %s %s", r.status_code, r.text[:500])
        return None
    except requests.RequestException as e:
        logger.error("SlickText create list error: %s", e)
        return None


def _delete_list(api_key: str, brand_id: str, list_id: int) -> None:
    url = f"{_V2_BASE}/brands/{brand_id}/lists/{list_id}"
    try:
        r = requests.delete(url, headers=_headers(api_key), timeout=30)
        if r.status_code not in (200, 204):
            logger.warning("SlickText delete list %s: %s %s", list_id, r.status_code, r.text[:200])
    except requests.RequestException as e:
        logger.warning("SlickText delete list error: %s", e)


def _add_contacts_to_list(
    api_key: str,
    brand_id: str,
    list_id: int,
    contact_ids: List[int],
) -> int:
    if not contact_ids:
        return 0
    url = f"{_V2_BASE}/brands/{brand_id}/lists/contacts"
    body = [{"contact_id": cid, "lists": [list_id]} for cid in contact_ids]
    try:
        r = requests.post(url, headers=_headers(api_key), json=body, timeout=120)
        if r.status_code == 200:
            data = r.json() if r.text else {}
            return int(data.get("count") or len(body))
        logger.error("SlickText add to list failed: %s %s", r.status_code, r.text[:500])
        return 0
    except requests.RequestException as e:
        logger.error("SlickText add to list error: %s", e)
        return 0


def _create_campaign(
    api_key: str,
    brand_id: str,
    name: str,
    body_text: str,
    list_id: int,
) -> Optional[int]:
    url = f"{_V2_BASE}/brands/{brand_id}/campaigns/"
    payload = {
        "name": name[:120],
        "body": body_text,
        "media_url": None,
        "status": "send",
        "audience": {"contact_lists": [list_id]},
    }
    try:
        r = requests.post(url, headers=_headers(api_key), json=payload, timeout=60)
        if r.status_code == 200:
            data = r.json()
            cid = data.get("campaign_id")
            return int(cid) if cid is not None else None
        logger.error("SlickText create campaign failed: %s %s", r.status_code, r.text[:800])
        return None
    except requests.RequestException as e:
        logger.error("SlickText create campaign error: %s", e)
        return None


def run_live_show_campaign(
    *,
    api_key: str,
    brand_id: str,
    list_name: str,
    campaign_name: str,
    body_text: str,
    phones_e164: List[str],
    progress: Optional[Callable[[int, int, int], None]] = None,
    delete_temp_list: Optional[bool] = None,
) -> SlickTextCampaignResult:
    """
    Sync phones into a new list and fire one SlickText campaign (status send).

    Progress callback: (processed_count, succeeded_contact_ids, failed_contact_ids)
    """
    if delete_temp_list is None:
        # Default false: deleting the list immediately may break a queued campaign that still references it.
        delete_temp_list = os.getenv("SLICKTEXT_CAMPAIGN_DELETE_TEMP_LIST", "false").lower() == "true"

    if not phones_e164:
        return SlickTextCampaignResult(
            ok=False,
            campaign_id=None,
            list_id=None,
            contacts_synced=0,
            contacts_failed=0,
            error="No phone numbers to send to",
        )

    tmp_list_id = _create_list(
        api_key,
        brand_id,
        list_name,
        "Temporary audience for Zarna Live Show admin broadcast (auto-created)",
    )
    if not tmp_list_id:
        return SlickTextCampaignResult(
            ok=False, campaign_id=None, list_id=None, contacts_synced=0, contacts_failed=len(phones_e164), error="Could not create SlickText list"
        )

    contact_ids: List[int] = []
    failed = 0
    for i, raw in enumerate(phones_e164):
        mobile = raw.strip()
        if not mobile.startswith("+"):
            mobile = f"+{mobile.lstrip('+')}"
        cid = _ensure_contact_id(api_key, brand_id, mobile)
        if cid:
            contact_ids.append(cid)
        else:
            failed += 1
        if progress:
            progress(i + 1, len(contact_ids), failed)

    if not contact_ids:
        if delete_temp_list:
            _delete_list(api_key, brand_id, tmp_list_id)
        return SlickTextCampaignResult(
            ok=False,
            campaign_id=None,
            list_id=tmp_list_id,
            contacts_synced=0,
            contacts_failed=failed,
            error="No contacts could be synced to SlickText",
        )

    batch_size = 80
    added = 0
    for i in range(0, len(contact_ids), batch_size):
        chunk = contact_ids[i : i + batch_size]
        added += _add_contacts_to_list(api_key, brand_id, tmp_list_id, chunk)
    if added == 0:
        err = "SlickText did not add any contacts to the list (check API response logs)"
        if delete_temp_list:
            _delete_list(api_key, brand_id, tmp_list_id)
        return SlickTextCampaignResult(
            ok=False, campaign_id=None, list_id=tmp_list_id, contacts_synced=len(contact_ids), contacts_failed=failed, error=err
        )

    camp_id = _create_campaign(api_key, brand_id, campaign_name, body_text, tmp_list_id)

    if delete_temp_list:
        _delete_list(api_key, brand_id, tmp_list_id)

    if not camp_id:
        return SlickTextCampaignResult(
            ok=False,
            campaign_id=None,
            list_id=tmp_list_id,
            contacts_synced=len(contact_ids),
            contacts_failed=failed,
            error="Campaign create failed after list sync (list may still exist if delete failed)",
        )

    return SlickTextCampaignResult(
        ok=True,
        campaign_id=camp_id,
        list_id=tmp_list_id if not delete_temp_list else None,
        contacts_synced=len(contact_ids),
        contacts_failed=failed,
        error=None if failed == 0 else f"{failed} numbers could not be synced as contacts",
    )


def v2_configured() -> bool:
    from app.config import SLICKTEXT_API_KEY, SLICKTEXT_BRAND_ID
    return bool(SLICKTEXT_API_KEY and SLICKTEXT_BRAND_ID)
