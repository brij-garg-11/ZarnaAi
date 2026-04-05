"""Admin bulk-action routes — one-off jobs, data imports, blast management."""

from datetime import datetime, timezone
from flask import Response, jsonify, redirect as _redirect, request

from app.admin_auth import (
    check_admin_auth,
    get_db_connection,
    require_admin_auth_response,
)


def register_action_routes(bp):
    """Attach all /admin/actions/* routes to the given blueprint."""

    @bp.route("/admin/actions/sync-slicktext-dates", methods=["POST"])
    def sync_slicktext_dates():
        """One-off job: backfill contacts.created_at from SlickText subscribedDate."""
        if not check_admin_auth():
            return require_admin_auth_response()

        import time

        import requests as _req

        pub  = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
        priv = os.getenv("SLICKTEXT_PRIVATE_KEY", "")
        if not pub or not priv:
            return Response("SLICKTEXT_PUBLIC_KEY / SLICKTEXT_PRIVATE_KEY not set", status=503, mimetype="text/plain")

        conn = get_db_connection()
        if not conn:
            return Response("DB not configured", status=503, mimetype="text/plain")

        _BACKFILL_THRESHOLD = "2026-03-26"
        _TEXTWORDS = [(3185378, "zarna"), (4633842, "hello")]
        _PAGE_SIZE = 200

        def _parse_date(raw):
            if not raw:
                return None
            try:
                datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
                return raw.strip()
            except ValueError:
                return None

        def _stream():
            yield "SlickText → Postgres date sync starting…\n\n"
            seen = {}
            for tw_id, label in _TEXTWORDS:
                yield f"Fetching textword '{label}' (id={tw_id})…\n"
                offset, total = 0, None
                while True:
                    resp = _req.get(
                        "https://api.slicktext.com/v1/contacts/",
                        params={"textword": tw_id, "limit": _PAGE_SIZE, "offset": offset},
                        auth=(pub, priv),
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        yield f"  API error {resp.status_code}: {resp.text[:200]}\n"
                        break
                    data = resp.json()
                    if total is None:
                        total = data["meta"]["total"]
                        yield f"  Total subscribers: {total:,}\n"
                    contacts = data.get("contacts", [])
                    if not contacts:
                        break
                    for c in contacts:
                        number = (c.get("number") or "").strip()
                        if number and number not in seen:
                            seen[number] = _parse_date(c.get("subscribedDate"))
                    offset += _PAGE_SIZE
                    yield f"  Fetched {min(offset, total):,} / {total:,}\n"
                    if offset >= total:
                        break
                    time.sleep(0.1)

            yield f"\nTotal unique contacts: {len(seen):,}\n"
            yield "Upserting into Postgres…\n"

            inserted = skipped = 0
            try:
                with conn:
                    with conn.cursor() as cur:
                        for number, sub_date in seen.items():
                            if sub_date:
                                cur.execute(
                                    """
                                    INSERT INTO contacts (phone_number, source, created_at)
                                    VALUES (%s, 'slicktext', %s::timestamp)
                                    ON CONFLICT (phone_number) DO UPDATE
                                      SET created_at = EXCLUDED.created_at
                                    WHERE contacts.created_at::date >= %s::date
                                    """,
                                    (number, sub_date, _BACKFILL_THRESHOLD),
                                )
                            else:
                                cur.execute(
                                    "INSERT INTO contacts (phone_number, source) VALUES (%s, 'slicktext') ON CONFLICT DO NOTHING",
                                    (number,),
                                )
                            if cur.rowcount > 0:
                                inserted += 1
                            else:
                                skipped += 1
            except Exception as exc:
                conn.rollback()
                yield f"\nDB error: {exc}\n"
                conn.close()
                return
            conn.close()

            yield f"\nInserted / updated : {inserted:,}\n"
            yield f"Already correct    : {skipped:,}\n"
            yield "\nDone. Reload the Insights tab to see updated pre-bot metrics.\n"

        return Response(_stream(), mimetype="text/plain")


    @bp.route("/admin/actions/delete-blast/<int:blast_id>", methods=["POST"])
    def delete_blast(blast_id: int):
        """Delete a blast draft record (for removing tests/junk from analytics)."""
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return Response("DB not configured", status=503, mimetype="text/plain")
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM blast_drafts WHERE id = %s", (blast_id,))
                    deleted = cur.rowcount
            conn.close()
            if deleted:
                return Response("deleted", status=200, mimetype="text/plain")
            return Response("not found", status=404, mimetype="text/plain")
        except Exception as e:
            conn.rollback()
            conn.close()
            return Response(f"Error: {e}", status=500, mimetype="text/plain")


    @bp.route("/admin/actions/set-blast-category/<int:blast_id>", methods=["POST"])
    def set_blast_category(blast_id: int):
        """Set the category (friendly / sales / show) on a blast_drafts record."""
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return jsonify({"ok": False, "error": "DB not configured"}), 503
        try:
            data     = request.get_json(force=True)
            category = (data.get("category") or "").strip().lower()
            if category not in ("friendly", "sales", "show", ""):
                return jsonify({"ok": False, "error": "category must be friendly, sales, show, or empty"}), 400
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE blast_drafts SET blast_category = %s WHERE id = %s",
                        (category or None, blast_id),
                    )
            conn.close()
            return jsonify({"ok": True})
        except Exception as e:
            conn.rollback()
            conn.close()
            return jsonify({"ok": False, "error": str(e)}), 500


    @bp.route("/admin/actions/add-external-blast", methods=["POST"])
    def add_external_blast():
        """Insert a manually-entered blast record (e.g. from SlickText) into blast_drafts."""
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return jsonify({"ok": False, "error": "DB not configured"}), 503
        try:
            data = request.get_json(force=True)
            name          = (data.get("name") or "").strip()
            date_str      = (data.get("date") or "").strip()
            sent_count    = int(data.get("sent_count") or 0)
            opt_out_count = int(data.get("opt_out_count") or 0)
            lc            = data.get("link_clicks")
            manual_link_clicks = int(lc) if lc is not None else None
            if not name or not date_str or sent_count <= 0:
                return jsonify({"ok": False, "error": "name, date, and sent_count are required"}), 400
            # Parse the date and treat it as noon UTC so it shows the right calendar day
            from datetime import datetime as _dt
            sent_at = _dt.strptime(date_str, "%Y-%m-%d").replace(hour=12)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO blast_drafts
                          (name, body, status, sent_at, sent_count, total_recipients,
                           opt_out_count, manual_link_clicks, created_by, channel)
                        VALUES (%s, '', 'sent', %s, %s, %s, %s, %s, 'external', 'slicktext')
                        RETURNING id
                        """,
                        (name, sent_at, sent_count, sent_count, opt_out_count, manual_link_clicks),
                    )
                    new_id = cur.fetchone()[0]
            conn.close()
            return jsonify({"ok": True, "id": new_id})
        except Exception as e:
            conn.rollback()
            conn.close()
            return jsonify({"ok": False, "error": str(e)}), 500


    @bp.route("/admin/actions/mark-blasts", methods=["POST"])
    def mark_blasts():
        """One-off: mark existing preseed/blast messages (same text to 50+ people) as source='blast'."""
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return Response("DB not configured", status=503, mimetype="text/plain")
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE messages
                        SET source = 'blast'
                        WHERE source IS NULL
                          AND role = 'assistant'
                          AND text IN (
                            SELECT text FROM messages
                            WHERE role = 'assistant' AND source IS NULL
                            GROUP BY text
                            HAVING COUNT(DISTINCT phone_number) >= 50
                          )
                        """
                    )
                    marked = cur.rowcount
            conn.close()
            return Response(f"Marked {marked:,} blast rows as source='blast'.", status=200, mimetype="text/plain")
        except Exception as e:
            conn.rollback()
            conn.close()
            return Response(f"Error: {e}", status=500, mimetype="text/plain")


    @bp.route("/admin/actions/import-chat-transcripts", methods=["GET", "POST"])
    def import_chat_transcripts():
        """Upload SlickText CSV and import pre-bot chat history into messages table."""
        if not check_admin_auth():
            return require_admin_auth_response()

        if request.method == "GET":
            return Response(
                """<!doctype html><html><body style="font-family:sans-serif;max-width:600px;margin:60px auto;padding:20px">
                <h2>Import SlickText Chat Transcripts</h2>
                <p>Upload the CSV exported from SlickText Inbox. Only messages before March 27 will be imported.</p>
                <form method="POST" enctype="multipart/form-data">
                  <input type="file" name="csv_file" accept=".csv" required style="margin-bottom:16px;display:block">
                  <button type="submit" style="padding:10px 24px;background:#6366f1;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:15px">
                    Import CSV
                  </button>
                </form></body></html>""",
                mimetype="text/html",
            )

        # POST — process uploaded file
        import csv as _csv
        import io
        from datetime import timezone as _tz, timedelta as _td

        f = request.files.get("csv_file")
        if not f:
            return Response("No file uploaded.", status=400, mimetype="text/plain")

        ZARNA_NUMBER = "+18775532629"
        BOT_LAUNCH   = datetime(2026, 3, 27, tzinfo=timezone.utc)
        REPLY_WINDOW = 48 * 3600  # seconds

        _TZ_OFF = {"EDT": -4, "EST": -5, "PDT": -7, "PST": -8, "UTC": 0}

        def _parse_ts(raw):
            raw = (raw or "").strip()
            if not raw:
                return None
            parts = raw.rsplit(" ", 1)
            offset_h = _TZ_OFF.get(parts[1].upper(), -5) if len(parts) == 2 else -5
            try:
                naive = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
                return naive.replace(tzinfo=timezone(timedelta(hours=offset_h)))
            except ValueError:
                return None

        def _stream():
            yield "SlickText Chat Transcript Import\n\n"
            text_data = f.stream.read().decode("utf-8")
            rows = []
            for r in _csv.DictReader(io.StringIO(text_data)):
                dt = _parse_ts(r.get("Sent", ""))
                if not dt or dt >= BOT_LAUNCH:
                    continue
                from_num = (r.get("From") or "").strip()
                to_num   = (r.get("To")   or "").strip()
                body     = (r.get("Body") or "").strip()
                if not body:
                    continue
                if from_num == ZARNA_NUMBER:
                    role, phone = "assistant", to_num
                else:
                    role, phone = "user", from_num
                if phone:
                    rows.append((phone, role, body, dt))

            incoming = sum(1 for r in rows if r[1] == "user")
            outgoing = sum(1 for r in rows if r[1] == "assistant")
            fans     = len({r[0] for r in rows if r[1] == "user"})
            yield f"Pre-bot rows found : {len(rows):,}\n"
            yield f"Incoming (fans)    : {incoming:,} from {fans:,} unique fans\n"
            yield f"Outgoing (Zarna)   : {outgoing:,}\n\n"

            conn = get_db_connection()
            if not conn:
                yield "DB not configured.\n"
                return

            try:
                # Ensure source column
                with conn.cursor() as cur:
                    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'bot'")
                conn.commit()
                yield "DB schema ready.\n"

                # Insert rows
                inserted = 0
                with conn.cursor() as cur:
                    for phone, role, body, dt in rows:
                        cur.execute(
                            "INSERT INTO messages (phone_number, role, text, created_at, source) "
                            "VALUES (%s, %s, %s, %s, 'csv_import') ON CONFLICT DO NOTHING",
                            (phone, role, body, dt),
                        )
                        if cur.rowcount > 0:
                            inserted += 1
                conn.commit()
                yield f"Inserted : {inserted:,}  (skipped {len(rows) - inserted:,} dupes)\n\n"

                # Score reply metrics
                yield "Scoring reply metrics…\n"
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE messages AS m
                        SET
                          did_user_reply = EXISTS (
                            SELECT 1 FROM messages m2
                            WHERE m2.phone_number = m.phone_number
                              AND m2.role = 'user' AND m2.source = 'csv_import'
                              AND m2.created_at > m.created_at
                              AND m2.created_at <= m.created_at + INTERVAL '{REPLY_WINDOW} seconds'
                          ),
                          reply_delay_seconds = (
                            SELECT EXTRACT(EPOCH FROM (m2.created_at - m.created_at))::int
                            FROM messages m2
                            WHERE m2.phone_number = m.phone_number
                              AND m2.role = 'user' AND m2.source = 'csv_import'
                              AND m2.created_at > m.created_at
                            ORDER BY m2.created_at LIMIT 1
                          ),
                          went_silent_after = NOT EXISTS (
                            SELECT 1 FROM messages m2
                            WHERE m2.phone_number = m.phone_number
                              AND m2.role = 'user' AND m2.source = 'csv_import'
                              AND m2.created_at > m.created_at
                              AND m2.created_at <= m.created_at + INTERVAL '{REPLY_WINDOW} seconds'
                          )
                        WHERE m.role = 'assistant'
                          AND m.source = 'csv_import'
                          AND m.did_user_reply IS NULL
                        """
                    )
                    scored = cur.rowcount
                conn.commit()
                yield f"Scored   : {scored:,} outgoing messages\n\n"
                yield "Done! Reload the Insights tab and switch to Pre-bot to see real reply rates.\n"
            except Exception as exc:
                conn.rollback()
                yield f"Error: {exc}\n"
            finally:
                conn.close()

        return Response(_stream(), mimetype="text/plain")


    @bp.route("/admin/quizzes/kill-all", methods=["POST"])
    def kill_all_quizzes():
        """Immediately expire all active quiz sessions. Requires admin auth."""
        if not check_admin_auth():
            return require_admin_auth_response()
        conn = get_db_connection()
        if not conn:
            return Response("DB not configured", status=503)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE quiz_sessions SET expires_at = NOW() - INTERVAL '1 second' "
                        "WHERE expires_at IS NULL OR expires_at > NOW()"
                    )
                    killed = cur.rowcount
            conn.close()
            return Response(f"Killed {killed} active quiz session(s).", status=200, mimetype="text/plain")
        except Exception as e:
            conn.close()
            return Response(f"Error: {e}", status=500, mimetype="text/plain")
