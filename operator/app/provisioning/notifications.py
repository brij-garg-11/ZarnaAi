"""
Welcome-email sender for a newly provisioned creator.

Uses Resend (already in operator/requirements.txt and used by auth.py for
password resets). Safe to call with no API key — logs a warning and returns
without raising.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from ..db import get_conn

_log = logging.getLogger(__name__)

_FROM_DEFAULT = os.getenv("RESEND_FROM", "hello@zar.bot")
_DASHBOARD_URL = os.getenv("PROVISIONING_DASHBOARD_URL", "https://zarnaai.up.railway.app")


def _get_user_email(user_id: int) -> Optional[str]:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT email, name FROM operator_users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            return (row[0], row[1] if row else None) if row else None
    finally:
        conn.close()


def _format_phone(phone_number: str) -> str:
    """Pretty-print +15551234567 → +1 (555) 123-4567."""
    digits = "".join(ch for ch in (phone_number or "") if ch.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return phone_number or "(number pending)"


def send_welcome(user_id: int, phone_number: str) -> None:
    """
    Send the "your bot is live" email. Never raises — provisioning success
    is the important outcome; email failure is a notification bug, not a
    pipeline bug.
    """
    try:
        info = _get_user_email(user_id)
        if not info or not info[0]:
            _log.warning("notifications: no email on file for user_id=%s — skipping welcome", user_id)
            return
        to_email, display_name = info
        display_name = display_name or to_email.split("@")[0]

        api_key = os.getenv("RESEND_API_KEY", "")
        if not api_key:
            _log.warning("notifications: RESEND_API_KEY not set — welcome email skipped (user=%s)", to_email)
            return

        import resend  # deferred import keeps module cheap to import in tests
        resend.api_key = api_key

        pretty_phone = _format_phone(phone_number)
        subject = "Your AI texting bot is live"
        html = _build_html(display_name, pretty_phone, _DASHBOARD_URL)
        text = _build_text(display_name, pretty_phone, _DASHBOARD_URL)

        resend.Emails.send({
            "from": f"Zar <{_FROM_DEFAULT}>",
            "to": [to_email],
            "subject": subject,
            "html": html,
            "text": text,
        })
        _log.info("notifications: welcome email sent to %s", to_email)

    except Exception:
        _log.exception("notifications: send_welcome failed — ignoring")


def _build_html(name: str, phone: str, dashboard_url: str) -> str:
    return f"""\
<p>Hey {name},</p>
<p>Your AI texting bot is live. Here's what you need to get started:</p>
<ul>
  <li><strong>Your dedicated number:</strong> {phone}</li>
  <li><strong>Your dashboard:</strong> <a href="{dashboard_url}">{dashboard_url}</a></li>
</ul>
<p><strong>First 3 things to do</strong></p>
<ol>
  <li>Text your number yourself — see how your bot sounds. Iterate if anything feels off.</li>
  <li>Share your keyword with a small group of fans first. Watch their replies in the dashboard.</li>
  <li>Send your first blast from the dashboard once you're happy with the voice.</li>
</ol>
<p>Any questions, just reply to this email.</p>
<p>— Zar</p>
"""


def _build_text(name: str, phone: str, dashboard_url: str) -> str:
    return f"""Hey {name},

Your AI texting bot is live.

Your dedicated number: {phone}
Your dashboard: {dashboard_url}

First 3 things to do:
  1. Text your number yourself — see how your bot sounds.
  2. Share your keyword with a small group of fans first.
  3. Send your first blast from the dashboard once you're happy.

Any questions, just reply to this email.

— Zar
"""
