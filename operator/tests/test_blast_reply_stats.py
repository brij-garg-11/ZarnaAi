"""
Blast reply-rate stats: of the fans a blast was sent to, how many texted back
within the attribution window.

Attribution is via `blast_recipients` (one row per successful send, link or
not) joined to inbound `user` messages — so it covers Twilio and SlickText
blasts alike, unlike link-click CTR.

`messages` is a main-app table not created by the operator migrations, so we
create a minimal version here (mirrors test_analytics_report).
"""

import datetime

import pytest


@pytest.fixture()
def blast_data():
    from app.db import get_conn
    conn = get_conn(); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY, phone_number TEXT, role TEXT, text TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(), creator_slug TEXT,
                intent TEXT, conversation_turn INT, did_user_reply BOOLEAN)
        """)
        cur.execute("TRUNCATE messages")
        cur.execute("TRUNCATE blast_recipients, blast_drafts RESTART IDENTITY CASCADE")

        base = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # A sent Zarna blast with 3 recipients, and an unrelated other-tenant blast.
        cur.execute(
            "INSERT INTO blast_drafts (name, status, creator_slug) "
            "VALUES ('Tour blast','sent','zarna') RETURNING id"
        )
        zarna_blast = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO blast_drafts (name, status, creator_slug) "
            "VALUES ('Other blast','sent','other') RETURNING id"
        )
        other_blast = cur.fetchone()[0]

        for phone in ("+1001", "+1002", "+1003"):
            cur.execute(
                "INSERT INTO blast_recipients (blast_id, phone_number, sent_at) "
                "VALUES (%s, %s, %s)",
                (zarna_blast, phone, base),
            )
        cur.execute(
            "INSERT INTO blast_recipients (blast_id, phone_number, sent_at) "
            "VALUES (%s, %s, %s)",
            (other_blast, "+9001", base),
        )

        def msg(phone, slug, minutes):
            cur.execute(
                "INSERT INTO messages (phone_number, role, text, created_at, creator_slug) "
                "VALUES (%s,'user','x',%s,%s)",
                (phone, base + datetime.timedelta(minutes=minutes), slug),
            )

        # 1001 replied 30 min after the blast -> counts.
        msg("+1001", "zarna", 30)
        # 1002 replied 2 days later (within 72h) -> counts.
        msg("+1002", "zarna", 60 * 24 * 2)
        # 1003 only "replied" 5 days later (outside 72h) -> does NOT count,
        # plus a message BEFORE the blast which must never count.
        msg("+1003", "zarna", 60 * 24 * 5)
        msg("+1003", "zarna", -120)
        # Other-tenant fan replied — proves tenant scoping in the bulk map.
        msg("+9001", "other", 10)

    conn.close()
    return {"zarna_blast": zarna_blast, "other_blast": other_blast}


def test_single_blast_reply_rate(blast_data):
    from app.queries import get_blast_reply_stats
    rs = get_blast_reply_stats(blast_data["zarna_blast"])
    assert rs["recipients"] == 3
    assert rs["replies"] == 2          # 1001 + 1002, not 1003 (outside window)
    assert rs["reply_rate_pct"] == 67  # round(2/3*100)


def test_no_recipients_yields_none(blast_data):
    from app.queries import get_blast_reply_stats
    # A draft id with no recorded recipients (e.g. pre-tracking blast).
    rs = get_blast_reply_stats(999999)
    assert rs["recipients"] == 0
    assert rs["reply_rate_pct"] is None


def test_window_is_respected(blast_data):
    from app.queries import get_blast_reply_stats
    # Shrink the window so 1002's 2-day-late reply no longer counts.
    rs = get_blast_reply_stats(blast_data["zarna_blast"], window_hours=1)
    assert rs["replies"] == 1
    assert rs["reply_rate_pct"] == 33


def test_bulk_map_is_tenant_scoped(blast_data):
    from app.queries import get_blast_reply_stats_map
    m = get_blast_reply_stats_map(creator_slug="zarna")
    assert blast_data["zarna_blast"] in m
    assert blast_data["other_blast"] not in m   # other tenant excluded
    assert m[blast_data["zarna_blast"]]["replies"] == 2


def test_overview_aggregates_tenant_blasts(blast_data):
    from app.queries import get_blast_reply_overview
    ov = get_blast_reply_overview("zarna")
    assert ov["recipients"] == 3       # other-tenant recipient excluded
    assert ov["replies"] == 2
    assert ov["reply_rate_pct"] == 67
    assert ov["blasts_counted"] == 1


@pytest.fixture()
def performer(client, make_user):
    uid = make_user("perf@zarna.test", creator_slug="zarna", account_type="performer")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


def test_status_endpoint_includes_reply_rate(client, performer, blast_data):
    body = client.get(f"/api/blasts/{blast_data['zarna_blast']}/status").get_json()
    assert body["success"] is True
    assert body["reply_recipients"] == 3
    assert body["replies"] == 2
    assert body["reply_rate_pct"] == 67


def test_list_endpoint_includes_reply_rate(client, performer, blast_data):
    drafts = client.get("/api/blasts").get_json()["drafts"]
    row = next(d for d in drafts if d["id"] == blast_data["zarna_blast"])
    assert row["reply_rate_pct"] == 67
    assert row["replies"] == 2
