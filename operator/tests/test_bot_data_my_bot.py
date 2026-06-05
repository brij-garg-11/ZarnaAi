"""
My Bot redesign (Phase 1): the performer /api/bot-data endpoint persists and
returns the new SMS-profile + first-message (opt-in) fields, and always returns
the non-editable compliance footer for the greyed-out UI hint.
"""

import pytest


@pytest.fixture()
def performer(client, make_user):
    uid = make_user("perf@zarna.test", creator_slug="zarna", account_type="performer")
    with client.session_transaction() as sess:
        sess["operator_user_id"] = uid
    return uid


def test_save_and_load_new_my_bot_fields(client, performer):
    payload = {
        "name": "Zarna Garg",
        "description": "South Asian comedian",
        "links": {"tickets": "https://t.co/x", "website": "https://zarnagarg.com"},
        "banned_words": ["foo"],
        "sms_display_name": "Zarna",
        "profile_photo_url": "https://img.example/zarna.png",
        "send_contact_card": True,
        "first_message": "Hi! It's Zarna. Thanks for texting me.",
    }
    r = client.post("/api/bot-data", json=payload)
    assert r.status_code == 200, r.get_json()
    assert r.get_json().get("success") is True

    g = client.get("/api/bot-data")
    assert g.status_code == 200
    body = g.get_json()
    assert body["sms_display_name"] == "Zarna"
    assert body["profile_photo_url"] == "https://img.example/zarna.png"
    assert body["send_contact_card"] is True
    assert body["first_message"].startswith("Hi! It's Zarna")
    assert body["links"]["website"] == "https://zarnagarg.com"
    # Compliance footer is always present, includes the required disclosures.
    cf = body["compliance_footer"]
    assert "data rates" in cf.lower()
    assert "STOP" in cf and "HELP" in cf


def test_compliance_footer_present_without_config(client, performer):
    # Even with no DB row (falls back to file config), the footer is returned.
    g = client.get("/api/bot-data")
    assert g.status_code == 200
    assert "STOP" in g.get_json().get("compliance_footer", "")


def test_custom_links_round_trip_and_sanitization(client, performer):
    """Custom links (Item 3): well-formed rows persist; junk is dropped on save."""
    payload = {
        "name": "Zarna Garg",
        "custom_links": [
            {"label": "Cooking Course", "url": "https://learn.example/cooking",
             "when_to_send": "fan asks about cooking classes"},
            {"label": "Patreon", "url": "https://patreon.com/zarna", "when_to_send": ""},
            # These should be stripped by _sanitize_custom_links:
            {"label": "Bad scheme", "url": "javascript:alert(1)"},
            {"label": "", "url": "https://missing-label.example"},
            {"label": "No url", "url": ""},
        ],
    }
    r = client.post("/api/bot-data", json=payload)
    assert r.status_code == 200, r.get_json()

    body = client.get("/api/bot-data").get_json()
    links = body["custom_links"]
    assert len(links) == 2
    assert links[0]["label"] == "Cooking Course"
    assert links[0]["url"] == "https://learn.example/cooking"
    assert links[0]["when_to_send"] == "fan asks about cooking classes"
    assert links[1]["label"] == "Patreon"
    assert links[1]["when_to_send"] == ""
