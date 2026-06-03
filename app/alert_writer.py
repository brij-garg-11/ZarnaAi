"""
Fire-and-forget alert writer for the main SMS app.

Writes structured alerts to the operator DB (OPERATOR_DATABASE_URL) so
clients can see plain-English notifications in their Tech Messages tab.
Never raises — a DB failure here must never kill a webhook handler.

Alert fields:
  alert_type  — machine tag: 'ai_error', 'credits_exhausted', 'capacity_reject',
                'message_send_failed'
  severity    — 'info' | 'warning' | 'error'
  title       — short client-visible headline (no technical jargon)
  summary     — 1-2 sentence client-visible explanation
  detail      — operator-only internal context (error message, stack, etc.)
"""

from __future__ import annotations

import logging
import os
import threading

log = logging.getLogger(__name__)

# Only write alerts when OPERATOR_DATABASE_URL is configured. If it's missing
# (e.g. local dev without an operator DB), we log and skip silently.
_OPERATOR_DB_URL = os.getenv("OPERATOR_DATABASE_URL", "")


def write_alert(
    creator_slug: str,
    alert_type: str,
    severity: str,
    title: str,
    summary: str,
    detail: str | None = None,
) -> None:
    """Queue an alert insert in a background thread. Returns immediately."""
    if not _OPERATOR_DB_URL:
        log.debug("[ALERT] OPERATOR_DATABASE_URL not set — skipping alert: %s", title)
        return
    threading.Thread(
        target=_write_alert_sync,
        args=(creator_slug, alert_type, severity, title, summary, detail),
        daemon=True,
    ).start()


def _write_alert_sync(
    creator_slug: str,
    alert_type: str,
    severity: str,
    title: str,
    summary: str,
    detail: str | None,
) -> None:
    try:
        import psycopg2

        dsn = _OPERATOR_DB_URL.replace("postgres://", "postgresql://", 1)
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO client_alerts
                        (creator_slug, alert_type, severity, title, summary, detail)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (creator_slug, alert_type, severity, title, summary, detail),
                )
        log.info("[ALERT] wrote %s alert for slug=%s: %s", severity, creator_slug, title)
    except Exception as exc:
        # Never propagate — this must not affect the SMS pipeline.
        log.warning("[ALERT] failed to write alert: %s", exc)

    # Error-severity alerts auto-create a Notion task (if NOTION_TASKS_DB_ID is set).
    # Handled inline here because alert_writer runs in the main-app container,
    # which doesn't have the operator/ package on its Python path.
    if severity == "error":
        tasks_db = os.getenv("NOTION_TASKS_DB_ID", "").strip()
        notion_token = os.getenv("NOTION_TOKEN", "").strip()
        if tasks_db and notion_token:
            threading.Thread(
                target=_create_notion_task,
                args=(creator_slug, title, summary, detail, tasks_db, notion_token),
                daemon=True,
            ).start()


def _create_notion_task(
    creator_slug: str,
    title: str,
    summary: str,
    detail: str | None,
    tasks_db_id: str,
    notion_token: str,
) -> None:
    """Write a task row to the operator's Notion tasks database."""
    import json
    import urllib.request

    description = summary
    if detail:
        description += f"\n\nDetail: {detail}"

    def _rt(s: str) -> list:
        return [{"type": "text", "text": {"content": s[:2000]}}]

    payload = json.dumps({
        "parent": {"database_id": tasks_db_id},
        "properties": {
            "Name":   {"title": _rt(f"[{creator_slug}] {title}")},
            "Slug":   {"rich_text": _rt(creator_slug)},
            "Status": {"select": {"name": "To Do"}},
        },
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": _rt(description)},
            }
        ],
    }).encode()

    req = urllib.request.Request(
        "https://api.notion.com/v1/pages",
        data=payload,
        headers={
            "Authorization": f"Bearer {notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            log.info("[ALERT] Notion task created: %s for slug=%s", data.get("id"), creator_slug)
    except Exception as exc:
        log.debug("[ALERT] Notion task creation failed: %s", exc)
