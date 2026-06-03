"""
Tests for TwilioAdapter.send_reply sender selection.

Multi-tenant correctness hinges on this: when an explicit `from_number` is
given (SMB tenants, multi-tenant performers), the reply MUST go out from that
specific number — NOT via the shared A2P messaging service's arbitrary pooled
sender. When no from_number is given, the messaging service is used as before.
"""

from app.messaging.twilio_adapter import TwilioAdapter


class _FakeMessages:
    def __init__(self):
        self.created = []

    def create(self, **kwargs):
        self.created.append(kwargs)

        class _Msg:
            sid = "SMfakesid"
        return _Msg()


class _FakeClient:
    def __init__(self):
        self.messages = _FakeMessages()


def _adapter(with_service_sid: bool):
    a = TwilioAdapter(
        account_sid="ACfake",
        auth_token="tokenfake",
        from_number="+15550000000",
        messaging_service_sid="MGfake" if with_service_sid else "",
    )
    a._client = _FakeClient()
    return a


def test_explicit_from_number_bypasses_messaging_service():
    a = _adapter(with_service_sid=True)
    ok = a.send_reply("+15551112222", "hi there", from_number="+15553334444")
    assert ok is True
    kwargs = a._client.messages.created[0]
    assert kwargs["from_"] == "+15553334444"
    assert "messaging_service_sid" not in kwargs


def test_no_from_number_uses_messaging_service():
    a = _adapter(with_service_sid=True)
    ok = a.send_reply("+15551112222", "hi there")
    assert ok is True
    kwargs = a._client.messages.created[0]
    assert kwargs["messaging_service_sid"] == "MGfake"
    assert "from_" not in kwargs


def test_no_service_sid_uses_default_from():
    a = _adapter(with_service_sid=False)
    ok = a.send_reply("+15551112222", "hi there")
    assert ok is True
    kwargs = a._client.messages.created[0]
    assert kwargs["from_"] == "+15550000000"


def test_explicit_from_number_without_service_sid():
    a = _adapter(with_service_sid=False)
    a.send_reply("+15551112222", "hi there", from_number="+15553334444")
    kwargs = a._client.messages.created[0]
    assert kwargs["from_"] == "+15553334444"
