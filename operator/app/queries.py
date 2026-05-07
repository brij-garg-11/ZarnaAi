"""
Safe aggregate queries — never returns raw phone numbers.
All phone numbers are masked or only counted.
"""

from __future__ import annotations
from collections import Counter
from .db import get_conn
import psycopg2.extras


# ── Stats ──────────────────────────────────────────────────────────────────

def get_overview_stats(creator_slug: str = "") -> dict:
    conn = get_conn()
    # Never fall back to "zarna" — callers must pass an explicit, authorized slug.
    # An empty slug would silently expose Zarna's data to any unprovisioned account.
    slug = creator_slug
    if not slug:
        return {}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE creator_slug=%s", (slug,))
            total_subscribers = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND creator_slug=%s", (slug,))
            total_messages = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '24 hours'", (slug,))
            messages_today = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '7 days'", (slug,))
            messages_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM messages
                WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '14 days'
                  AND created_at < NOW()-INTERVAL '7 days'
            """, (slug,))
            messages_prev_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE creator_slug=%s AND created_at >= NOW()-INTERVAL '7 days'", (slug,))
            new_subs_week = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(DISTINCT phone_number) FROM contacts
                WHERE creator_slug=%s AND created_at >= NOW()-INTERVAL '14 days'
                  AND created_at < NOW()-INTERVAL '7 days'
            """, (slug,))
            new_subs_prev_week = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM messages WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '1 hour'", (slug,))
            messages_last_hour = cur.fetchone()[0]

            cur.execute("""
                SELECT DATE(created_at AT TIME ZONE 'America/New_York') as day, COUNT(*) as cnt
                FROM messages WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '30 days'
                GROUP BY day ORDER BY day
            """, (slug,))
            messages_by_day = [(str(r["day"]), r["cnt"]) for r in cur.fetchall()]

            cur.execute("""
                SELECT EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/New_York')::int as hr,
                       COUNT(*) as cnt
                FROM messages WHERE role='user' AND creator_slug=%s AND created_at >= NOW()-INTERVAL '30 days'
                GROUP BY hr ORDER BY hr
            """, (slug,))
            hour_map = {r["hr"]: r["cnt"] for r in cur.fetchall()}
            messages_by_hour = [hour_map.get(h, 0) for h in range(24)]

            cur.execute("""
                SELECT UNNEST(fan_tags) as tag, COUNT(*) as cnt
                FROM contacts WHERE creator_slug=%s AND fan_tags IS NOT NULL AND fan_tags != '{}'
                GROUP BY tag ORDER BY cnt DESC LIMIT 20
            """, (slug,))
            tag_breakdown = [(r["tag"], r["cnt"]) for r in cur.fetchall()]

            cur.execute("SELECT phone_number FROM contacts WHERE creator_slug=%s", (slug,))
            all_phones = [r[0] for r in cur.fetchall()]
            area_codes = Counter()
            for p in all_phones:
                digits = "".join(c for c in p if c.isdigit())
                if len(digits) == 11 and digits[0] == "1":
                    area_codes[digits[1:4]] += 1
                elif len(digits) == 10:
                    area_codes[digits[:3]] += 1
            top_area_codes = area_codes.most_common(10)

            cur.execute("SELECT COUNT(*) FROM contacts WHERE creator_slug=%s AND fan_memory IS NOT NULL AND fan_memory != ''", (slug,))
            profiled_fans = cur.fetchone()[0]

        return {
            "total_subscribers": total_subscribers,
            "total_messages": total_messages,
            "messages_today": messages_today,
            "messages_week": messages_week,
            "messages_prev_week": messages_prev_week,
            "new_subs_week": new_subs_week,
            "new_subs_prev_week": new_subs_prev_week,
            "messages_last_hour": messages_last_hour,
            "messages_by_day": messages_by_day,
            "messages_by_hour": messages_by_hour,
            "tag_breakdown": tag_breakdown,
            "top_area_codes": top_area_codes,
            "profiled_fans": profiled_fans,
        }
    finally:
        conn.close()


# ── Audience count (no PII) ────────────────────────────────────────────────

def _slug_clause(slug: str | None, table_alias: str = "") -> tuple[str, list]:
    """Return a ('AND <alias>creator_slug = %s', [slug]) pair when ``slug`` is
    set, otherwise ('', []). ``table_alias`` is optional (e.g. 'c.' for a
    joined query). Keeps multi-tenant filtering DRY across the audience
    helpers below."""
    if not slug:
        return "", []
    prefix = f"{table_alias}" if table_alias else ""
    return f" AND {prefix}creator_slug = %s", [slug]


def count_audience(
    audience_type: str,
    audience_filter: str,
    sample_pct: int = 100,
    creator_slug: str | None = None,
) -> int:
    """Count how many subscribers match the given filters — no phones returned.

    ``creator_slug`` scopes the query to a single tenant. ``None`` returns
    cross-tenant totals (super-admin only)."""
    slug_sql, slug_params = _slug_clause(creator_slug)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if audience_type == "tag" and audience_filter:
                cur.execute(
                    "SELECT COUNT(DISTINCT phone_number) FROM contacts "
                    "WHERE %s = ANY(fan_tags)" + slug_sql,
                    tuple([audience_filter.lower(), *slug_params]),
                )
            elif audience_type == "location" and audience_filter:
                cur.execute(
                    "SELECT COUNT(DISTINCT phone_number) FROM contacts "
                    "WHERE LOWER(fan_location) LIKE %s" + slug_sql,
                    tuple([f"%{audience_filter.lower()}%", *slug_params]),
                )
            elif audience_type == "show" and audience_filter:
                try:
                    show_id = int(audience_filter)
                    # Signups join through live_shows to enforce slug match.
                    if creator_slug:
                        cur.execute(
                            """SELECT COUNT(DISTINCT lss.phone_number)
                               FROM   live_show_signups lss
                               JOIN   live_shows ls ON ls.id = lss.show_id
                               WHERE  lss.show_id = %s AND ls.creator_slug = %s""",
                            (show_id, creator_slug),
                        )
                    else:
                        cur.execute(
                            "SELECT COUNT(DISTINCT phone_number) FROM live_show_signups WHERE show_id = %s",
                            (show_id,),
                        )
                except (ValueError, TypeError):
                    cur.execute("SELECT 0")
            elif audience_type == "tier" and audience_filter:
                valid_tiers = {"superfan", "engaged", "lurker", "dormant"}
                if audience_filter.lower() in valid_tiers:
                    cur.execute(
                        "SELECT COUNT(DISTINCT phone_number) FROM contacts "
                        "WHERE fan_tier = %s" + slug_sql,
                        tuple([audience_filter.lower(), *slug_params]),
                    )
                else:
                    cur.execute("SELECT 0")
            elif audience_type == "compound" and audience_filter:
                import json
                try:
                    filters = json.loads(audience_filter)
                    clauses, params = _build_compound_clauses(filters, creator_slug=creator_slug)
                    cur.execute(
                        f"SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE {' AND '.join(clauses)}",
                        params,
                    )
                except Exception:
                    cur.execute("SELECT 0")
            elif audience_type == "engaged":
                # Smart Send: top-N most engaged contacts. audience_filter
                # holds the desired N (defaults to 100 if blank).
                try:
                    n = max(1, min(5000, int(audience_filter or 100)))
                except (ValueError, TypeError):
                    n = 100
                cur.execute(
                    """SELECT COUNT(*) FROM (
                         SELECT phone_number FROM contacts
                         WHERE engagement_score > 0
                           AND phone_number NOT LIKE 'whatsapp:%%'
                         """ + slug_sql + """
                         ORDER BY engagement_score DESC
                         LIMIT %s
                       ) sub""",
                    tuple([*slug_params, n]),
                )
            else:
                cur.execute(
                    "SELECT COUNT(DISTINCT phone_number) FROM contacts WHERE TRUE" + slug_sql,
                    tuple(slug_params),
                )
            total = cur.fetchone()[0]

        if audience_type == "random" and 0 < sample_pct < 100:
            return max(1, round(total * sample_pct / 100))
        return total
    finally:
        conn.close()


def get_audience_phones(
    audience_type: str,
    audience_filter: str,
    sample_pct: int = 100,
    creator_slug: str | None = None,
) -> list[str]:
    """Fetch phone numbers for blast — internal use only, never sent to frontend.

    ``creator_slug`` restricts recipients to the caller's tenant. ``None`` keeps
    the historical cross-tenant behaviour (super-admin only)."""
    slug_sql, slug_params = _slug_clause(creator_slug)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            optout_set = _get_optouts(cur)

            if audience_type == "tag" and audience_filter:
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE %s = ANY(fan_tags) AND phone_number NOT LIKE 'whatsapp:%%'" + slug_sql,
                    tuple([audience_filter.lower(), *slug_params]),
                )
            elif audience_type == "location" and audience_filter:
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE LOWER(fan_location) LIKE %s AND phone_number NOT LIKE 'whatsapp:%%'" + slug_sql,
                    tuple([f"%{audience_filter.lower()}%", *slug_params]),
                )
            elif audience_type == "show" and audience_filter:
                try:
                    show_id = int(audience_filter)
                    if creator_slug:
                        cur.execute(
                            """SELECT DISTINCT lss.phone_number
                               FROM   live_show_signups lss
                               JOIN   live_shows ls ON ls.id = lss.show_id
                               WHERE  lss.show_id = %s AND ls.creator_slug = %s""",
                            (show_id, creator_slug),
                        )
                    else:
                        cur.execute(
                            "SELECT DISTINCT phone_number FROM live_show_signups WHERE show_id = %s",
                            (show_id,),
                        )
                except (ValueError, TypeError):
                    cur.execute("SELECT DISTINCT phone_number FROM contacts WHERE FALSE")
            elif audience_type == "tier" and audience_filter:
                valid_tiers = {"superfan", "engaged", "lurker", "dormant"}
                if audience_filter.lower() in valid_tiers:
                    cur.execute(
                        "SELECT DISTINCT phone_number FROM contacts "
                        "WHERE fan_tier = %s AND phone_number NOT LIKE 'whatsapp:%%'" + slug_sql,
                        tuple([audience_filter.lower(), *slug_params]),
                    )
                else:
                    cur.execute("SELECT DISTINCT phone_number FROM contacts WHERE FALSE")
            elif audience_type == "compound" and audience_filter:
                import json
                try:
                    filters = json.loads(audience_filter)
                    clauses, params = _build_compound_clauses(filters, creator_slug=creator_slug)
                    cur.execute(
                        f"SELECT DISTINCT phone_number FROM contacts WHERE {' AND '.join(clauses)}",
                        params,
                    )
                except Exception:
                    cur.execute("SELECT DISTINCT phone_number FROM contacts WHERE FALSE")
            elif audience_type == "engaged":
                try:
                    n = max(1, min(5000, int(audience_filter or 100)))
                except (ValueError, TypeError):
                    n = 100
                cur.execute(
                    """SELECT phone_number FROM contacts
                       WHERE engagement_score > 0
                         AND phone_number NOT LIKE 'whatsapp:%%'
                       """ + slug_sql + """
                       ORDER BY engagement_score DESC
                       LIMIT %s""",
                    tuple([*slug_params, n]),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT phone_number FROM contacts "
                    "WHERE phone_number NOT LIKE 'whatsapp:%%'" + slug_sql,
                    tuple(slug_params),
                )

            phones = [r[0] for r in cur.fetchall() if r[0] not in optout_set]

        if audience_type == "random" and 0 < sample_pct < 100:
            import random
            k = max(1, round(len(phones) * sample_pct / 100))
            return random.sample(phones, min(k, len(phones)))
        return phones
    finally:
        conn.close()


def get_audience_fan_data(phones: list[str]) -> dict[str, dict]:
    """Return {phone: {fan_name, fan_location}} for a list of phones.

    Used by blast_sender to resolve {{name}} / {{location}} merge tags
    at send time without re-running the full audience query.
    Only fetches the two fields needed — no memory blob returned.
    """
    if not phones:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT phone_number,
                       COALESCE(fan_name, '')     AS fan_name,
                       COALESCE(fan_location, '') AS fan_location
                FROM   contacts
                WHERE  phone_number = ANY(%s)
                """,
                (phones,),
            )
            return {
                row[0]: {"fan_name": row[1], "fan_location": row[2]}
                for row in cur.fetchall()
            }
    finally:
        conn.close()


def _build_compound_clauses(
    filters: list[dict],
    creator_slug: str | None = None,
) -> tuple[list[str], list]:
    """
    Convert a list of filter dicts into SQL WHERE clauses (AND logic).
    Each filter: {"type": "tier"|"tag"|"location", "value": "..."}
    Returns (clauses, params) — always includes the whatsapp exclusion and,
    when ``creator_slug`` is provided, a tenant scope.
    """
    clauses = ["phone_number NOT LIKE 'whatsapp:%%'"]
    params: list = []
    if creator_slug:
        clauses.append("creator_slug = %s")
        params.append(creator_slug)
    valid_tiers = {"superfan", "engaged", "lurker", "dormant"}
    for f in filters:
        ftype = (f.get("type") or "").strip()
        val = (f.get("value") or "").strip()
        if not ftype or not val:
            continue
        if ftype == "tier" and val.lower() in valid_tiers:
            clauses.append("fan_tier = %s")
            params.append(val.lower())
        elif ftype == "tag":
            clauses.append("%s = ANY(fan_tags)")
            params.append(val.lower())
        elif ftype == "location":
            clauses.append("LOWER(COALESCE(fan_location,'')) LIKE %s")
            params.append(f"%{val.lower()}%")
    return clauses, params


def _get_optouts(cur) -> set:
    cur.execute("SELECT phone_number FROM broadcast_optouts")
    return {r[0] for r in cur.fetchall()}


# ── Audience tags list ──────────────────────────────────────────────────────

def get_all_tags(creator_slug: str | None = None) -> list[str]:
    """Distinct fan_tags visible to the caller. Tenant-scoped when
    ``creator_slug`` is provided."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if creator_slug:
                cur.execute("""
                    SELECT DISTINCT UNNEST(fan_tags) as tag FROM contacts
                    WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                      AND creator_slug = %s
                    ORDER BY tag
                """, (creator_slug,))
            else:
                cur.execute("""
                    SELECT DISTINCT UNNEST(fan_tags) as tag FROM contacts
                    WHERE fan_tags IS NOT NULL AND fan_tags != '{}'
                    ORDER BY tag
                """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


# ── Blast drafts ───────────────────────────────────────────────────────────

def list_blast_drafts(creator_slug: str | None = None) -> list[dict]:
    """Return recent blast drafts.

    When ``creator_slug`` is provided, results are scoped to that tenant so
    team members only see their own project's drafts. ``None`` returns
    everything — intended for super-admin / cross-tenant views.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            base = """
                SELECT id, name, body, channel, audience_type, audience_filter,
                       audience_sample_pct, status, scheduled_at, sent_at,
                       sent_count, failed_count, total_recipients, created_by, created_at,
                       COALESCE(creator_slug, '')          AS creator_slug,
                       COALESCE(media_url, '')             AS media_url,
                       COALESCE(link_url, '')              AS link_url,
                       COALESCE(tracked_link_slug, '')     AS tracked_link_slug,
                       COALESCE(is_quiz, FALSE)            AS is_quiz,
                       COALESCE(quiz_correct_answer, '')   AS quiz_correct_answer,
                       COALESCE(blast_context_note, '')    AS blast_context_note
                FROM blast_drafts
            """
            if creator_slug:
                cur.execute(base + " WHERE creator_slug = %s ORDER BY updated_at DESC LIMIT 100",
                            (creator_slug,))
            else:
                cur.execute(base + " ORDER BY updated_at DESC LIMIT 100")
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_blast_draft(draft_id: int, creator_slug: str | None = None) -> dict | None:
    """Fetch one draft. If ``creator_slug`` is provided, returns None for
    drafts belonging to a different tenant."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if creator_slug:
                cur.execute(
                    "SELECT * FROM blast_drafts WHERE id = %s AND creator_slug = %s",
                    (draft_id, creator_slug),
                )
            else:
                cur.execute("SELECT * FROM blast_drafts WHERE id = %s", (draft_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def save_blast_draft(*, name: str, body: str, channel: str, audience_type: str,
                     audience_filter: str, sample_pct: int, created_by: str,
                     media_url: str = "", link_url: str = "",
                     tracked_link_slug: str = "",
                     is_quiz: bool = False, quiz_correct_answer: str = "",
                     blast_context_note: str = "",
                     creator_slug: str | None = None,
                     draft_id: int | None = None) -> int:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                if draft_id:
                    cur.execute("""
                        UPDATE blast_drafts
                        SET name=%s, body=%s, channel=%s, audience_type=%s,
                            audience_filter=%s, audience_sample_pct=%s,
                            media_url=%s, link_url=%s, tracked_link_slug=%s,
                            is_quiz=%s, quiz_correct_answer=%s,
                            blast_context_note=%s,
                            status='draft', updated_at=NOW()
                        WHERE id=%s
                    """, (name, body, channel, audience_type, audience_filter, sample_pct,
                          media_url, link_url, tracked_link_slug,
                          is_quiz, quiz_correct_answer, blast_context_note, draft_id))
                    return draft_id
                else:
                    cur.execute("""
                        INSERT INTO blast_drafts
                          (name, body, channel, audience_type, audience_filter,
                           audience_sample_pct, media_url, link_url, tracked_link_slug,
                           is_quiz, quiz_correct_answer, blast_context_note, status,
                           created_by, creator_slug)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'draft',%s,%s) RETURNING id
                    """, (name, body, channel, audience_type, audience_filter, sample_pct,
                          media_url, link_url, tracked_link_slug,
                          is_quiz, quiz_correct_answer, blast_context_note,
                          created_by, (creator_slug or None)))
                    return cur.fetchone()[0]
    finally:
        conn.close()


def schedule_blast(draft_id: int, send_at) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE blast_drafts
                    SET status='scheduled', scheduled_at=%s, updated_at=NOW()
                    WHERE id=%s
                """, (send_at, draft_id))
    finally:
        conn.close()


def mark_blast_started(draft_id: int, total: int) -> None:
    """Record the exact moment the send loop begins and set total_recipients."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET started_at=NOW(), total_recipients=%s, updated_at=NOW() WHERE id=%s",
                    (total, draft_id),
                )
    finally:
        conn.close()


def mark_blast_progress(draft_id: int, sent: int, failed: int) -> None:
    """Write incremental sent/failed counts mid-loop so the UI can poll progress."""
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET sent_count=%s, failed_count=%s, updated_at=NOW() WHERE id=%s",
                    (sent, failed, draft_id),
                )
    finally:
        conn.close()


def mark_blast_sent(draft_id: int, sent: int, failed: int, total: int) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE blast_drafts
                    SET status='sent', sent_at=NOW(), sent_count=%s,
                        failed_count=%s, total_recipients=%s, updated_at=NOW()
                    WHERE id=%s
                """, (sent, failed, total, draft_id))
    finally:
        conn.close()


def mark_blast_cancelled(draft_id: int) -> None:
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE blast_drafts SET status='cancelled', updated_at=NOW() WHERE id=%s",
                    (draft_id,),
                )
    finally:
        conn.close()


def claim_pending_scheduled_blasts() -> list[dict]:
    """
    Atomically claim scheduled blasts by marking them 'sending' in one
    UPDATE … RETURNING statement with FOR UPDATE SKIP LOCKED.
    This prevents two gunicorn workers from double-firing the same blast.
    Only blasts with a non-empty body are claimed; empty-body blasts are
    cancelled automatically so they stop looping forever.
    """
    conn = get_conn()
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                # Cancel ghost blasts with no body so they never fire again
                cur.execute("""
                    UPDATE blast_drafts SET status='cancelled', updated_at=NOW()
                    WHERE status='scheduled'
                      AND (body IS NULL OR body = '')
                      AND scheduled_at <= NOW()
                """)
                if cur.rowcount:
                    import logging
                    logging.getLogger(__name__).warning(
                        "Cancelled %d scheduled blast(s) with empty body", cur.rowcount)

                # Atomically claim ready blasts for this worker only
                cur.execute("""
                    UPDATE blast_drafts SET status='sending', updated_at=NOW()
                    WHERE id IN (
                        SELECT id FROM blast_drafts
                        WHERE status='scheduled' AND scheduled_at <= NOW()
                        ORDER BY scheduled_at
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING *
                """)
                return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_pending_scheduled_blasts():
    """Legacy alias — use claim_pending_scheduled_blasts for new code."""
    return claim_pending_scheduled_blasts()


# ── Live shows (read-only, PII-free) ───────────────────────────────────────

def list_shows(creator_slug: str | None = None) -> list[dict]:
    """List live shows.

    When ``creator_slug`` is provided, results are scoped to that tenant so
    team members only ever see their own project's shows. Passing ``None``
    returns everything — intended for super-admin / cross-tenant views.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if creator_slug:
                cur.execute("""
                    SELECT s.id, s.name, s.status, s.keyword, s.deliver_as,
                           s.event_category, s.window_start, s.window_end,
                           s.event_timezone, s.created_at, s.creator_slug,
                           (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                    FROM live_shows s
                    WHERE s.creator_slug = %s
                    ORDER BY s.created_at DESC
                """, (creator_slug,))
            else:
                cur.execute("""
                    SELECT s.id, s.name, s.status, s.keyword, s.deliver_as,
                           s.event_category, s.window_start, s.window_end,
                           s.event_timezone, s.created_at, s.creator_slug,
                           (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                    FROM live_shows s
                    ORDER BY s.created_at DESC
                """)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        import logging
        logging.warning("list_shows error: %s", e)
        return []
    finally:
        conn.close()


def get_show(show_id: int, creator_slug: str | None = None) -> dict | None:
    """Fetch one show. If ``creator_slug`` is provided, enforce tenant scoping
    — returns None when the show belongs to a different tenant."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if creator_slug:
                cur.execute("""
                    SELECT s.id, s.name, s.status, s.keyword, s.use_keyword_only,
                           s.deliver_as, s.event_category, s.event_timezone,
                           s.window_start, s.window_end, s.created_at, s.creator_slug,
                           (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                    FROM live_shows s
                    WHERE s.id = %s AND s.creator_slug = %s
                """, (show_id, creator_slug))
            else:
                cur.execute("""
                    SELECT s.id, s.name, s.status, s.keyword, s.use_keyword_only,
                           s.deliver_as, s.event_category, s.event_timezone,
                           s.window_start, s.window_end, s.created_at, s.creator_slug,
                           (SELECT COUNT(*) FROM live_show_signups x WHERE x.show_id = s.id) AS signup_count
                    FROM live_shows s
                    WHERE s.id = %s
                """, (show_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        import logging
        logging.warning("get_show error: %s", e)
        return None
    finally:
        conn.close()
