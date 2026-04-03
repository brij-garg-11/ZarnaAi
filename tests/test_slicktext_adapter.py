"""
Tests for the SlickText v2 adapter.

- Inbound parsing uses v2 inbox_message_received payload format.
- Contact lookup is mocked (no network calls).
- Outbound send is noted as unsupported in v2 docs; send_reply returns False.
- Webhook endpoint is tested end-to-end against the Flask app.
"""

import sys
import os
import json
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.messaging.slicktext_adapter import SlickTextAdapter


def _v2_adapter(**kwargs):
    """Force v2 mode even when real v1 keys exist in the environment."""
    return SlickTextAdapter(public_key="", private_key="", **kwargs)


# ---------------------------------------------------------------------------
# Inbound parsing
# ---------------------------------------------------------------------------

def test_parse_valid_payload():
    """v2 inbox_message_received payload — contact lookup is mocked."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    payload = {
        "name": "inbox_message_received",
        "data": {
            "contact_id": 1111111,
            "_brand_id": 99999,
            "last_message": "tell me a joke about Indian moms",
            "last_message_direction": "incoming",
        }
    }

    with patch("app.messaging.slicktext_adapter.requests.get") as mock_get:
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"mobile_number": "+15554449998"}
        )
        phone, message = adapter.parse_inbound(payload)

    assert phone == "+15554449998", f"Expected phone, got: {phone}"
    assert message == "tell me a joke about Indian moms"
    print("✓ parse_inbound: valid v2 payload + contact lookup")


def test_parse_outgoing_message_ignored():
    """Outgoing messages (our replies) should not trigger a response loop."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    payload = {
        "name": "inbox_message_received",
        "data": {
            "contact_id": 1111,
            "last_message": "reply we sent",
            "last_message_direction": "outgoing",
        }
    }
    phone, message = adapter.parse_inbound(payload)
    assert phone is None
    assert message is None
    print("✓ parse_inbound: outgoing messages ignored")


def test_parse_empty_payload():
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")
    phone, message = adapter.parse_inbound({})
    assert phone is None
    assert message is None
    print("✓ parse_inbound: empty payload returns (None, None)")


def test_parse_contact_lookup_fails():
    """If contact lookup returns non-200, phone should be None."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    payload = {
        "name": "inbox_message_received",
        "data": {
            "contact_id": 9999,
            "last_message": "hello",
            "last_message_direction": "incoming",
        }
    }

    with patch("app.messaging.slicktext_adapter.requests.get") as mock_get:
        mock_get.return_value = MagicMock(status_code=404, text="Not Found")
        phone, message = adapter.parse_inbound(payload)

    assert phone is None
    assert message == "hello"
    print("✓ parse_inbound: phone is None when contact lookup fails")


# ---------------------------------------------------------------------------
# Outbound send
# ---------------------------------------------------------------------------

def test_send_reply_unconfigured():
    """send_reply returns False gracefully when keys not configured."""
    adapter = _v2_adapter(api_key="", brand_id="")
    result = adapter.send_reply("+15554449998", "test")
    assert result is False
    print("✓ send_reply: returns False when credentials missing")


def test_send_reply_success():
    """send_reply POSTs to /brands/{brand_id}/messages with mobile_number + body."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    with patch("app.messaging.slicktext_adapter.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        result = adapter.send_reply("+15554449998", "Here is a Zarna joke!")

    assert result is True
    call_kwargs = mock_post.call_args
    assert call_kwargs.kwargs["json"]["mobile_number"] == "+15554449998"
    assert call_kwargs.kwargs["json"]["body"] == "Here is a Zarna joke!"
    print("✓ send_reply: correct payload sent, returns True on 200")


def test_send_reply_api_failure():
    """Paid plan required — test account returns 409, should return False."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    with patch("app.messaging.slicktext_adapter.requests.post") as mock_post:
        mock_post.return_value = MagicMock(
            status_code=409,
            text="Please contact your account owner and have them upgrade."
        )
        result = adapter.send_reply("+15554449998", "test")

    assert result is False
    print("✓ send_reply: returns False on 409 (unpaid plan)")


def test_send_reply_retries_on_429_then_succeeds():
    """429 on first attempt, 200 on second — should return True and call POST twice."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    responses = [
        MagicMock(status_code=429, text="rate limited"),
        MagicMock(status_code=200),
    ]
    with patch("app.messaging.slicktext_adapter.requests.post", side_effect=responses) as mock_post, \
         patch("app.messaging.slicktext_adapter.time.sleep"):
        result = adapter.send_reply("+15554449998", "test")

    assert result is True
    assert mock_post.call_count == 2
    print("✓ send_reply: retries on 429, succeeds on second attempt")


def test_send_reply_fails_after_three_429s():
    """Three consecutive 429s — should return False after exhausting all retries."""
    adapter = _v2_adapter(api_key="testkey", brand_id="99999")

    responses = [MagicMock(status_code=429, text="rate limited")] * 3
    with patch("app.messaging.slicktext_adapter.requests.post", side_effect=responses) as mock_post, \
         patch("app.messaging.slicktext_adapter.time.sleep"):
        result = adapter.send_reply("+15554449998", "test")

    assert result is False
    assert mock_post.call_count == 3
    print("✓ send_reply: returns False after 3 rate-limit retries")


# ---------------------------------------------------------------------------
# Flask webhook endpoint (end-to-end)
# ---------------------------------------------------------------------------

def test_slicktext_webhook_endpoint():
    """
    POST a v2 inbox_message_received payload to /slicktext/webhook.
    Contact lookup is mocked. Should return 200 + {status: ok}.
    """
    import main as app_module

    with patch("app.messaging.slicktext_adapter.requests.get") as mock_get, patch.object(
        app_module,
        "_process_slicktext_message",
    ):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {"mobile_number": "+15559876543"}
        )

        app_module.slicktext = _v2_adapter(api_key="test-api-key", brand_id="99999")
        client = app_module.app.test_client()

        payload = {
            "name": "inbox_message_received",
            "data": {
                "contact_id": 1111111,
                "_brand_id": 99999,
                "last_message": "give me a joke",
                "last_message_direction": "incoming",
            }
        }

        response = client.post(
            "/slicktext/webhook",
            data=json.dumps(payload),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ok"
        print("✓ /slicktext/webhook: returns 200 + {status: ok}")


def test_slicktext_webhook_bad_payload():
    """Missing contact_id / message → parse_inbound returns None; webhook responds 200 ignored."""
    import main as app_module

    app_module.slicktext = _v2_adapter(api_key="test-api-key", brand_id="99999")
    client = app_module.app.test_client()

    response = client.post(
        "/slicktext/webhook",
        data=json.dumps({"name": "inbox_message_received", "data": {}}),
        content_type="application/json",
    )
    assert response.status_code == 200
    assert response.get_json()["status"] == "ignored"
    print("✓ /slicktext/webhook: returns 200 + ignored on missing fields")


if __name__ == "__main__":
    print("=== SlickText v2 Adapter Tests ===\n")
    test_parse_valid_payload()
    test_parse_outgoing_message_ignored()
    test_parse_empty_payload()
    test_parse_contact_lookup_fails()
    test_send_reply_unconfigured()
    test_send_reply_success()
    test_send_reply_api_failure()
    print("\n--- Webhook endpoint tests (may trigger Gemini calls) ---")
    test_slicktext_webhook_endpoint()
    test_slicktext_webhook_bad_payload()
    print("\nAll SlickText v2 adapter tests passed.")
