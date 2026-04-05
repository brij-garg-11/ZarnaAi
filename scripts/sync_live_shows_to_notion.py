#!/usr/bin/env python3
"""
Sync live show rows from Postgres into a Notion database (weekly cron or one-off).

Env (required):
  NOTION_TOKEN           Internal integration secret
  NOTION_DATABASE_ID     Database UUID from the Notion URL
  DATABASE_URL           Same Postgres as production

Env (optional — property names must match your Notion database exactly):
  NOTION_PROP_TITLE=Show
  NOTION_PROP_DATE=Show date/time
  NOTION_PROP_STAGE=Stage                    # Notion "status" type; optional
  NOTION_PROP_APP_ID=App show ID             # number — required for stable matching
  NOTION_PROP_SIGNUPS=Signups
  NOTION_PROP_CHANNELS=Signup channels       # rich_text summary
  NOTION_PROP_KEYWORD=Keyword
  NOTION_PROP_CATEGORY=Event category
  NOTION_PROP_BROADCAST_COUNT=Broadcast count
  NOTION_PROP_BLAST_SENT=Blast sent (last)
  NOTION_PROP_BLAST_FAILED=Blast failed (last)
  NOTION_PROP_BLAST_STATUS=Blast status (last)
  NOTION_PROP_SYNCED_AT=Data synced at
  NOTION_PROP_WINDOW_END=Window end
  NOTION_PROP_REENGAGEMENT=Fan re-engagement rate
  NOTION_PROP_AVG_MSGS=Avg msgs per fan
  NOTION_PROP_VELOCITY=Signup velocity
  NOTION_PROP_BLAST_OUTCOME=Broadcast outcome
  NOTION_PROP_VS_AVG=vs avg signups

  NOTION_SYNC_STAGE=0|1          default 0 — set 1 after Stage options include mapped names
  NOTION_STAGE_DRAFT=Planned     maps DB status draft
  NOTION_STAGE_LIVE=Live         maps live
  NOTION_STAGE_ENDED=Done        maps ended

  NOTION_API_VERSION=2022-06-28
  NOTION_ONLY_SHOW_IDS=          comma-separated app show ids (limit sync for testing)

Run:
  python scripts/sync_live_shows_to_notion.py
  python scripts/sync_live_shows_to_notion.py --check-schema   # no DATABASE_URL needed

Notion setup:
  1. Add the properties above (types: title already exists; add numbers, rich_text, date as named).
  2. For Stage, add options matching NOTION_STAGE_* or keep NOTION_SYNC_STAGE=0.
  3. Connect the integration to the database (Connections).

DATABASE_URL:
  Use the exact same value as your production Zarna web service — one Postgres database
  holds live_shows, signups, and broadcasts. The Notion cron does not need a different URL.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv

    _here = os.path.dirname(os.path.abspath(__file__))
    load_dotenv()
    load_dotenv(os.path.join(_here, "..", ".env"))
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [notion_sync] %(message)s",
    stream=sys.stdout,
    force=True,
)
_log = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"


def _banner(msg: str) -> None:
    line = f"=== NOTION_SYNC {msg} ==="
    print(line, flush=True)
    _log.info(msg)


def _normalize_uuid(raw: str) -> str:
    s = raw.strip().replace("-", "")
    if len(s) != 32 or not re.fullmatch(r"[0-9a-fA-F]{32}", s):
        return raw.strip()
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


def _headers() -> Dict[str, str]:
    token = (os.getenv("NOTION_TOKEN") or "").strip()
    ver = (os.getenv("NOTION_API_VERSION") or "2022-06-28").strip()
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": ver,
        "Content-Type": "application/json",
    }


def _get_db_conn():
    import psycopg2

    url = (os.getenv("DATABASE_URL") or "").strip()
    if not url:
        _log.error("DATABASE_URL is not set")
        sys.exit(1)
    return psycopg2.connect(url.replace("postgres://", "postgresql://", 1))


def ensure_notion_page_id_column(conn) -> None:
    """Idempotent — allows this script to run on a cron-only Railway service."""
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE live_shows ADD COLUMN IF NOT EXISTS notion_page_id TEXT"
            )


def notion_retrieve_database(database_id: str) -> Dict[str, Any]:
    rid = _normalize_uuid(database_id)
    r = requests.get(
        f"{NOTION_API}/databases/{rid}",
        headers=_headers(),
        timeout=60,
    )
    if r.status_code != 200:
        _log.error("Notion retrieve database failed: %s %s", r.status_code, r.text[:2000])
        sys.exit(1)
    return r.json()


def notion_query_by_app_show_id(
    database_id: str, prop_app_id: str, app_show_id: int
) -> Optional[str]:
    rid = _normalize_uuid(database_id)
    body = {
        "filter": {"property": prop_app_id, "number": {"equals": int(app_show_id)}},
        "page_size": 1,
    }
    r = requests.post(
        f"{NOTION_API}/databases/{rid}/query",
        headers=_headers(),
        json=body,
        timeout=60,
    )
    if r.status_code != 200:
        _log.warning(
            "Notion query by App show ID failed (add a Number property %r): %s %s",
            prop_app_id,
            r.status_code,
            r.text[:500],
        )
        return None
    results = r.json().get("results") or []
    if not results:
        return None
    return results[0].get("id")


def notion_create_page(database_id: str, properties: Dict[str, Any]) -> str:
    rid = _normalize_uuid(database_id)
    body = {"parent": {"database_id": rid}, "properties": properties}
    r = requests.post(
        f"{NOTION_API}/pages",
        headers=_headers(),
        json=body,
        timeout=60,
    )
    if r.status_code != 200:
        _log.error("Notion create page failed: %s %s", r.status_code, r.text[:2000])
        raise RuntimeError(r.text)
    return r.json()["id"]


def notion_update_page(page_id: str, properties: Dict[str, Any]) -> None:
    pid = _normalize_uuid(page_id)
    r = requests.patch(
        f"{NOTION_API}/pages/{pid}",
        headers=_headers(),
        json={"properties": properties},
        timeout=60,
    )
    if r.status_code != 200:
        _log.error("Notion update page failed: %s %s", r.status_code, r.text[:2000])
        raise RuntimeError(r.text)


def _rich_text(content: str) -> Dict[str, Any]:
    content = (content or "")[:1900]
    if not content:
        return {"rich_text": []}
    return {
        "rich_text": [
            {"type": "text", "text": {"content": content}},
        ]
    }


def _title(content: str) -> Dict[str, Any]:
    content = (content or "Untitled show")[:1900]
    return {
        "title": [
            {"type": "text", "text": {"content": content}},
        ]
    }


def _number_val(n: Optional[int]) -> Dict[str, Any]:
    if n is None:
        return {"number": None}
    return {"number": int(n)}


def _date_val(dt: Optional[datetime]) -> Dict[str, Any]:
    if dt is None:
        return {"date": None}
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    iso = dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    return {"date": {"start": iso}}


def _date_datetime_val(dt: Optional[datetime]) -> Dict[str, Any]:
    """Notion date property can include time."""
    if dt is None:
        return {"date": None}
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    iso = dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return {"date": {"start": iso}}


def _status_val(name: str) -> Dict[str, Any]:
    return {"status": {"name": name}}


def _select_val(name: str) -> Dict[str, Any]:
    return {"select": {"name": name}}


def _map_stage(db_status: str) -> Optional[str]:
    if os.getenv("NOTION_SYNC_STAGE", "0").strip().lower() not in ("1", "true", "yes", "on"):
        return None
    s = (db_status or "").strip().lower()
    draft = (os.getenv("NOTION_STAGE_DRAFT") or "Planned").strip()
    live = (os.getenv("NOTION_STAGE_LIVE") or "Live").strip()
    ended = (os.getenv("NOTION_STAGE_ENDED") or "Done").strip()
    if s == "draft":
        return draft
    if s == "live":
        return live
    if s == "ended":
        return ended
    return draft


def _prop_type(schema: Dict[str, Any], name: str) -> Optional[str]:
    props = schema.get("properties") or {}
    p = props.get(name)
    if not p:
        return None
    return p.get("type")


def build_properties_for_show(
    schema: Dict[str, Any],
    show: Dict[str, Any],
    channel_parts: str,
    last_job: Optional[Dict[str, Any]],
    analytics: Optional[Dict[str, Any]] = None,
    avg_signups: float = 0.0,
) -> Dict[str, Any]:
    env = os.getenv

    def pname(key: str, default: str) -> str:
        return (env(key) or default).strip()

    title_p = pname("NOTION_PROP_TITLE", "Show")
    date_p = pname("NOTION_PROP_DATE", "Show date/time")
    stage_p = pname("NOTION_PROP_STAGE", "Stage")
    app_id_p = pname("NOTION_PROP_APP_ID", "App show ID")
    signups_p = pname("NOTION_PROP_SIGNUPS", "Signups")
    channels_p = pname("NOTION_PROP_CHANNELS", "Signup channels")
    keyword_p = pname("NOTION_PROP_KEYWORD", "Keyword")
    category_p = pname("NOTION_PROP_CATEGORY", "Event category")
    bcount_p = pname("NOTION_PROP_BROADCAST_COUNT", "Broadcast count")
    bsent_p = pname("NOTION_PROP_BLAST_SENT", "Blast sent (last)")
    bfail_p = pname("NOTION_PROP_BLAST_FAILED", "Blast failed (last)")
    bstat_p = pname("NOTION_PROP_BLAST_STATUS", "Blast status (last)")
    synced_p = pname("NOTION_PROP_SYNCED_AT", "Data synced at")
    winend_p = pname("NOTION_PROP_WINDOW_END", "Window end")
    reeng_p = pname("NOTION_PROP_REENGAGEMENT", "Fan re-engagement rate")
    avgmsgs_p = pname("NOTION_PROP_AVG_MSGS", "Avg msgs per fan")
    velocity_p = pname("NOTION_PROP_VELOCITY", "Signup velocity")
    boutcome_p = pname("NOTION_PROP_BLAST_OUTCOME", "Broadcast outcome")
    vsavg_p = pname("NOTION_PROP_VS_AVG", "vs avg signups")

    out: Dict[str, Any] = {}
    name = show.get("name") or f"Show {show.get('id')}"

    t = _prop_type(schema, title_p)
    if t == "title":
        out[title_p] = _title(name)

    dt = show.get("window_start") or show.get("created_at")
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    dtp = _prop_type(schema, date_p)
    if dtp == "date" and dt:
        out[date_p] = _date_val(dt if isinstance(dt, datetime) else None)

    stage_name = _map_stage(str(show.get("status") or ""))
    stp = _prop_type(schema, stage_p)
    if stage_name and stp == "status":
        out[stage_p] = _status_val(stage_name)
    elif stage_name and stp == "select":
        out[stage_p] = _select_val(stage_name)

    ntp = _prop_type(schema, app_id_p)
    if ntp == "number":
        out[app_id_p] = _number_val(int(show["id"]))

    if _prop_type(schema, signups_p) == "number":
        out[signups_p] = _number_val(int(show.get("signup_total") or 0))

    if _prop_type(schema, channels_p) == "rich_text" and channel_parts:
        out[channels_p] = _rich_text(channel_parts)

    kw = (show.get("keyword") or "").strip()
    if kw and _prop_type(schema, keyword_p) == "rich_text":
        out[keyword_p] = _rich_text(kw)

    cat = (show.get("event_category") or "").strip()
    if cat and _prop_type(schema, category_p) == "rich_text":
        out[category_p] = _rich_text(cat)
    elif cat and _prop_type(schema, category_p) == "select":
        out[category_p] = _select_val(cat)

    job_n = int(show.get("broadcast_job_count") or 0)
    if _prop_type(schema, bcount_p) == "number":
        out[bcount_p] = _number_val(job_n)

    if last_job:
        sent = last_job.get("sent_count")
        failed = last_job.get("failed_count")
        status = (last_job.get("status") or "").strip()
        if _prop_type(schema, bsent_p) == "number" and sent is not None:
            out[bsent_p] = _number_val(int(sent))
        if _prop_type(schema, bfail_p) == "number" and failed is not None:
            out[bfail_p] = _number_val(int(failed))
        if status and _prop_type(schema, bstat_p) == "rich_text":
            out[bstat_p] = _rich_text(status)
        elif status and _prop_type(schema, bstat_p) == "select":
            out[bstat_p] = _select_val(status)

    now = datetime.now(timezone.utc)
    if _prop_type(schema, synced_p) == "date":
        out[synced_p] = _date_datetime_val(now)

    we = show.get("window_end")
    if isinstance(we, str):
        we = datetime.fromisoformat(we.replace("Z", "+00:00"))
    if we and _prop_type(schema, winend_p) == "date":
        out[winend_p] = _date_val(we if isinstance(we, datetime) else None)

    # --- analytics columns ---
    a = analytics or {}
    signup_total = int(show.get("signup_total") or 0)

    if _prop_type(schema, reeng_p) == "rich_text":
        out[reeng_p] = _rich_text(_fmt_reengagement(a))

    if _prop_type(schema, avgmsgs_p) == "rich_text":
        out[avgmsgs_p] = _rich_text(_fmt_avg_msgs(a))

    if _prop_type(schema, velocity_p) == "rich_text":
        out[velocity_p] = _rich_text(_fmt_velocity(a))

    if _prop_type(schema, boutcome_p) == "rich_text":
        out[boutcome_p] = _rich_text(_fmt_broadcast_outcome(last_job))

    if _prop_type(schema, vsavg_p) == "rich_text":
        out[vsavg_p] = _rich_text(_fmt_vs_avg(signup_total, avg_signups))

    return out


def fetch_shows(conn) -> List[Dict[str, Any]]:
    only = (os.getenv("NOTION_ONLY_SHOW_IDS") or "").strip()
    id_filter = ""
    params: Tuple[Any, ...] = ()
    if only:
        ids = [int(x.strip()) for x in only.split(",") if x.strip().isdigit()]
        if ids:
            id_filter = " WHERE s.id = ANY(%s::int[])"
            params = (ids,)

    q = f"""
        SELECT s.id, s.name, s.keyword, s.status, s.window_start, s.window_end,
               s.event_timezone, s.event_category, s.deliver_as, s.use_keyword_only,
               s.created_at, s.updated_at, s.notion_page_id,
               (SELECT COUNT(*)::int FROM live_show_signups x WHERE x.show_id = s.id) AS signup_total,
               (SELECT COUNT(*)::int FROM live_broadcast_jobs b WHERE b.show_id = s.id) AS broadcast_job_count
        FROM live_shows s
        {id_filter}
        ORDER BY s.created_at DESC
    """
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(q, params)
        rows = [dict(r) for r in cur.fetchall()]
    return rows


def fetch_signup_channels(conn, show_ids: List[int]) -> Dict[int, str]:
    if not show_ids:
        return {}
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT show_id, COALESCE(NULLIF(TRIM(channel), ''), '(default)') AS ch, COUNT(*)::int AS n
            FROM live_show_signups
            WHERE show_id = ANY(%s::int[])
            GROUP BY show_id, COALESCE(NULLIF(TRIM(channel), ''), '(default)')
            """,
            (show_ids,),
        )
        raw = cur.fetchall()
    by_show: Dict[int, List[str]] = {i: [] for i in show_ids}
    for r in raw:
        sid = int(r["show_id"])
        by_show.setdefault(sid, []).append(f"{r['ch']}: {r['n']}")
    return {sid: ", ".join(sorted(parts)) for sid, parts in by_show.items()}


def fetch_latest_jobs(conn, show_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not show_ids:
        return {}
    import psycopg2.extras

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (show_id) show_id, status, sent_count, failed_count,
                   total_recipients, created_at
            FROM live_broadcast_jobs
            WHERE show_id = ANY(%s::int[])
            ORDER BY show_id, created_at DESC
            """,
            (show_ids,),
        )
        rows = cur.fetchall()
    return {int(r["show_id"]): dict(r) for r in rows}


def fetch_show_analytics(conn, show_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Four analytics queries in one go for all synced show IDs.
    Returns dict keyed by show_id with:
      total_signups, fans_texted_7d, avg_msgs, first_30_pct, first_30_count, window_start
    """
    if not show_ids:
        return {}
    import psycopg2.extras

    result: Dict[int, Dict[str, Any]] = {sid: {} for sid in show_ids}

    # 1. Fan re-engagement — how many signed-up fans sent any message within 7d of signup
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                lss.show_id,
                COUNT(DISTINCT lss.phone_number)::int AS total_signups,
                COUNT(DISTINCT
                    CASE WHEN EXISTS (
                        SELECT 1 FROM messages m
                        WHERE m.phone_number = lss.phone_number
                          AND m.role = 'user'
                          AND m.created_at > lss.signed_up_at
                          AND m.created_at <= lss.signed_up_at + INTERVAL '7 days'
                    ) THEN lss.phone_number END
                )::int AS fans_texted_7d
            FROM live_show_signups lss
            WHERE lss.show_id = ANY(%s::int[])
            GROUP BY lss.show_id
            """,
            (show_ids,),
        )
        for r in cur.fetchall():
            sid = int(r["show_id"])
            result[sid]["total_signups"] = int(r["total_signups"] or 0)
            result[sid]["fans_texted_7d"] = int(r["fans_texted_7d"] or 0)

    # 2. Avg messages per fan (all-time from show's audience)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                lss.show_id,
                ROUND(AVG(COALESCE(mc.msg_count, 0))::numeric, 1)::float AS avg_msgs
            FROM live_show_signups lss
            LEFT JOIN (
                SELECT m.phone_number, COUNT(*)::int AS msg_count
                FROM messages m
                WHERE m.role = 'user'
                  AND m.phone_number = ANY(
                    SELECT phone_number FROM live_show_signups
                    WHERE show_id = ANY(%s::int[])
                  )
                GROUP BY m.phone_number
            ) mc ON mc.phone_number = lss.phone_number
            WHERE lss.show_id = ANY(%s::int[])
            GROUP BY lss.show_id
            """,
            (show_ids, show_ids),
        )
        for r in cur.fetchall():
            sid = int(r["show_id"])
            result[sid]["avg_msgs"] = float(r["avg_msgs"] or 0.0)

    # 3. Signup velocity — % of signups who joined in first 30 min after window_start
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                s.id AS show_id,
                s.window_start,
                COUNT(lss.phone_number)::int AS total_signups,
                COUNT(CASE
                    WHEN s.window_start IS NOT NULL
                         AND lss.signed_up_at <= s.window_start + INTERVAL '30 minutes'
                    THEN 1 END
                )::int AS first_30_count
            FROM live_shows s
            LEFT JOIN live_show_signups lss ON lss.show_id = s.id
            WHERE s.id = ANY(%s::int[])
            GROUP BY s.id, s.window_start
            """,
            (show_ids,),
        )
        for r in cur.fetchall():
            sid = int(r["show_id"])
            result[sid]["window_start"] = r["window_start"]
            total = int(r["total_signups"] or 0)
            first30 = int(r["first_30_count"] or 0)
            if r["window_start"] and total > 0:
                result[sid]["first_30_pct"] = round(first30 / total * 100, 1)
                result[sid]["first_30_count"] = first30
            else:
                result[sid]["first_30_pct"] = None
                result[sid]["first_30_count"] = 0

    return result


def fetch_avg_ended_signups(conn) -> float:
    """Average signup count across all ended shows — used for above/below avg comparison."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT AVG(cnt)::float AS avg_signups
            FROM (
                SELECT s.id, COUNT(lss.phone_number)::int AS cnt
                FROM live_shows s
                LEFT JOIN live_show_signups lss ON lss.show_id = s.id
                WHERE s.status = 'ended'
                GROUP BY s.id
            ) t
            """
        )
        row = cur.fetchone()
        if row and row[0] is not None:
            return float(row[0])
    return 0.0


# ---------------------------------------------------------------------------
# Analytics text formatters
# ---------------------------------------------------------------------------

def _fmt_reengagement(a: Dict[str, Any]) -> str:
    total = a.get("total_signups", 0)
    texted = a.get("fans_texted_7d", 0)
    if not total:
        return "No signups"
    pct = round(texted / total * 100, 1)
    return f"{pct}% ({texted}/{total} fans texted AI within 7d)"


def _fmt_avg_msgs(a: Dict[str, Any]) -> str:
    avg = a.get("avg_msgs", 0.0)
    return f"{avg} msgs/fan (all-time from show audience)"


def _fmt_velocity(a: Dict[str, Any]) -> str:
    pct = a.get("first_30_pct")
    if pct is None:
        return "No window start — cannot compute"
    count = a.get("first_30_count", 0)
    total = a.get("total_signups", 0)
    return f"{pct}% signed up in first 30 min ({count}/{total})"


def _fmt_broadcast_outcome(last_job: Optional[Dict[str, Any]]) -> str:
    if not last_job:
        return "No broadcast"
    sent = int(last_job.get("sent_count") or 0)
    failed = int(last_job.get("failed_count") or 0)
    total = int(last_job.get("total_recipients") or 0)
    status = (last_job.get("status") or "").strip()
    if total == 0:
        return status or "No broadcast"
    pct = round(sent / total * 100) if total else 0
    parts = f"{sent} sent"
    if failed:
        parts += f", {failed} failed"
    parts += f" ({pct}% success)"
    return parts


def _fmt_vs_avg(signup_total: int, avg_signups: float) -> str:
    if avg_signups <= 0:
        return f"{signup_total} signups (no comparison data yet)"
    avg_r = round(avg_signups, 1)
    if signup_total > avg_signups * 1.1:
        arrow = "↑ above avg"
    elif signup_total < avg_signups * 0.9:
        arrow = "↓ below avg"
    else:
        arrow = "≈ near avg"
    return f"{arrow} (avg: {avg_r}, this show: {signup_total})"


def save_notion_page_id(conn, show_id: int, page_id: str) -> None:
    pid = _normalize_uuid(page_id)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE live_shows SET notion_page_id = %s, updated_at = NOW() WHERE id = %s",
                (pid, show_id),
            )


# (env override key, default column name, acceptable Notion types, tier: required|recommended)
_SYNC_COLUMN_SPEC: Tuple[Tuple[str, str, Tuple[str, ...], str], ...] = (
    ("NOTION_PROP_TITLE", "Show", ("title",), "required"),
    ("NOTION_PROP_APP_ID", "App show ID", ("number",), "required"),
    ("NOTION_PROP_DATE", "Show date/time", ("date",), "recommended"),
    ("NOTION_PROP_STAGE", "Stage", ("status", "select"), "optional"),
    ("NOTION_PROP_SIGNUPS", "Signups", ("number",), "recommended"),
    ("NOTION_PROP_CHANNELS", "Signup channels", ("rich_text",), "recommended"),
    ("NOTION_PROP_KEYWORD", "Keyword", ("rich_text",), "recommended"),
    ("NOTION_PROP_CATEGORY", "Event category", ("rich_text", "select"), "recommended"),
    ("NOTION_PROP_BROADCAST_COUNT", "Broadcast count", ("number",), "recommended"),
    ("NOTION_PROP_BLAST_SENT", "Blast sent (last)", ("number",), "recommended"),
    ("NOTION_PROP_BLAST_FAILED", "Blast failed (last)", ("number",), "recommended"),
    ("NOTION_PROP_BLAST_STATUS", "Blast status (last)", ("rich_text", "select"), "recommended"),
    ("NOTION_PROP_SYNCED_AT", "Data synced at", ("date",), "recommended"),
    ("NOTION_PROP_WINDOW_END", "Window end", ("date",), "recommended"),
    ("NOTION_PROP_REENGAGEMENT", "Fan re-engagement rate", ("rich_text",), "analytics"),
    ("NOTION_PROP_AVG_MSGS", "Avg msgs per fan", ("rich_text",), "analytics"),
    ("NOTION_PROP_VELOCITY", "Signup velocity", ("rich_text",), "analytics"),
    ("NOTION_PROP_BLAST_OUTCOME", "Broadcast outcome", ("rich_text",), "analytics"),
    ("NOTION_PROP_VS_AVG", "vs avg signups", ("rich_text",), "analytics"),
)


def _resolved_prop_name(env_key: str, default: str) -> str:
    return (os.getenv(env_key) or default).strip()


def run_check_schema() -> None:
    """Print Notion DB properties and a checklist — needs NOTION_TOKEN + NOTION_DATABASE_ID only."""
    token = (os.getenv("NOTION_TOKEN") or "").strip()
    database_id = (os.getenv("NOTION_DATABASE_ID") or "").strip()
    if not token or not database_id:
        _log.error("NOTION_TOKEN and NOTION_DATABASE_ID are required for --check-schema")
        sys.exit(1)

    schema = notion_retrieve_database(database_id)
    props = schema.get("properties") or {}

    print("\n=== Your Notion database (as the API sees it) ===\n", flush=True)
    for name in sorted(props.keys(), key=lambda x: x.lower()):
        print(f"  {name!r}  →  type={props[name].get('type')!r}", flush=True)

    print("\n=== Fix checklist for Zarna live-show sync ===\n", flush=True)
    print(
        "Add any row marked MISSING or WRONG TYPE (Database → + New property).\n"
        "Names must match exactly, or set the NOTION_PROP_* env var to your custom name.\n",
        flush=True,
    )

    for env_key, default_name, ok_types, tier in _SYNC_COLUMN_SPEC:
        pname = _resolved_prop_name(env_key, default_name)
        actual = props.get(pname)
        atype = actual.get("type") if actual else None
        if atype is None:
            status = "MISSING — add this property"
        elif atype not in ok_types:
            status = f"WRONG TYPE (is {atype!r}, need one of {ok_types})"
        else:
            status = "OK"
        print(f"  [{tier:11}] {pname!r}  →  {status}", flush=True)

    print(
        "\nLeave Owner, Notes, Important updates, and pre/during/post checkboxes as manual fields; "
        "the sync does not overwrite them.\n"
        "Stage: keep NOTION_SYNC_STAGE=0 until your Stage options include Planned, Live, Done "
        "(or set NOTION_STAGE_* to match your options), then set NOTION_SYNC_STAGE=1.\n",
        flush=True,
    )


def main_sync() -> None:
    _banner("START")
    token = (os.getenv("NOTION_TOKEN") or "").strip()
    database_id = (os.getenv("NOTION_DATABASE_ID") or "").strip()
    if not token or not database_id:
        _log.error("NOTION_TOKEN and NOTION_DATABASE_ID are required")
        sys.exit(1)

    app_id_prop = (os.getenv("NOTION_PROP_APP_ID") or "App show ID").strip()

    schema = notion_retrieve_database(database_id)
    title_prop = (os.getenv("NOTION_PROP_TITLE") or "Show").strip()
    if _prop_type(schema, title_prop) != "title":
        _log.error(
            "Notion database must have a title property named %r (set NOTION_PROP_TITLE). "
            "Schema has: %s",
            title_prop,
            list((schema.get("properties") or {}).keys()),
        )
        sys.exit(1)
    if _prop_type(schema, app_id_prop) != "number":
        _log.warning(
            "Add a Number property %r to your Notion database for stable upserts "
            "and filtering. Continuing: will rely on stored notion_page_id only.",
            app_id_prop,
        )

    conn = _get_db_conn()
    try:
        ensure_notion_page_id_column(conn)
        shows = fetch_shows(conn)
        ids = [int(s["id"]) for s in shows]
        channels = fetch_signup_channels(conn, ids)
        jobs = fetch_latest_jobs(conn, ids)
        analytics_all = fetch_show_analytics(conn, ids)
        avg_signups = fetch_avg_ended_signups(conn)
        _log.info("avg_ended_signups=%.1f (used for vs-avg column)", avg_signups)

        _log.info("Syncing %d live show(s) to Notion", len(shows))
        ok = 0
        for show in shows:
            sid = int(show["id"])
            ch = channels.get(sid, "")
            last_job = jobs.get(sid)
            a = analytics_all.get(sid, {})
            props = build_properties_for_show(schema, show, ch, last_job, a, avg_signups)
            if not props:
                _log.warning("No properties to write for show_id=%s", sid)
                continue

            page_id = (show.get("notion_page_id") or "").strip()
            if not page_id and _prop_type(schema, app_id_prop) == "number":
                page_id = notion_query_by_app_show_id(database_id, app_id_prop, sid) or ""

            try:
                if page_id:
                    notion_update_page(page_id, props)
                    _log.info("Updated Notion page for show_id=%s page=%s", sid, page_id[:8])
                else:
                    page_id = notion_create_page(database_id, props)
                    _log.info("Created Notion page for show_id=%s page=%s", sid, page_id[:8])
                save_notion_page_id(conn, sid, page_id)
                ok += 1
            except Exception:
                _log.exception("Failed show_id=%s", sid)

        _banner(f"DONE synced={ok}/{len(shows)}")
    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Sync live_shows from Postgres into a Notion database."
    )
    p.add_argument(
        "--check-schema",
        action="store_true",
        help="List Notion properties and show which columns to add (no DATABASE_URL needed).",
    )
    args = p.parse_args()
    if args.check_schema:
        run_check_schema()
    else:
        main_sync()


if __name__ == "__main__":
    main()
