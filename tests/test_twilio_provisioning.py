"""
Unit tests for operator/app/provisioning/twilio_numbers.py

The operator service uses its own top-level `app` package, which collides with
the main app's `app/` package — so we cannot `import app.provisioning...` inside
this (main-app) pytest session. Instead we load the dependency-free
twilio_numbers module directly by file path and drive it with a fake Twilio
client. twilio_numbers.py has no relative imports, so this loads cleanly.
"""

import importlib.util
import os

import pytest

_MODULE_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "operator",
    "app",
    "provisioning",
    "twilio_numbers.py",
)


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "twilio_numbers_under_test", os.path.abspath(_MODULE_PATH)
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


tn = _load_module()


# ---------------------------------------------------------------------------
# Fake Twilio client mirroring the SDK surface twilio_numbers.py uses.
# Every interesting call is appended to `client.calls` so tests can assert
# both *that* a call happened and the *order* of calls.
# ---------------------------------------------------------------------------

class _FakeNumber:
    def __init__(self, phone_number):
        self.phone_number = phone_number


class _FakePurchased:
    def __init__(self, phone_number, sid):
        self.phone_number = phone_number
        self.sid = sid


class _Local:
    def __init__(self, client):
        self._client = client

    def list(self, **kwargs):
        self._client.calls.append(("local.list", kwargs))
        ac = kwargs.get("area_code")
        phones = self._client.pool.get(ac, self._client.pool.get(None, []))
        return [_FakeNumber(p) for p in phones]


class _Available:
    def __init__(self, client, country):
        self.country = country
        self.local = _Local(client)


class _SvcNumbers:
    def __init__(self, client, service_sid):
        self._client = client
        self._service_sid = service_sid

    def create(self, **kwargs):
        self._client.calls.append(("svc.attach", self._service_sid, kwargs))


class _Service:
    def __init__(self, client, service_sid):
        self.phone_numbers = _SvcNumbers(client, service_sid)


class _V1:
    def __init__(self, client):
        self._client = client

    def services(self, service_sid):
        return _Service(self._client, service_sid)


class _Messaging:
    def __init__(self, client):
        self.v1 = _V1(client)


class _IncomingCtx:
    def __init__(self, client, sid):
        self._client = client
        self._sid = sid

    def delete(self):
        self._client.calls.append(("incoming.delete", self._sid))


class _IncomingNumbers:
    """Callable (for delete) AND has .create (for purchase)."""

    def __init__(self, client):
        self._client = client

    def create(self, **kwargs):
        self._client.calls.append(("incoming.create", kwargs))
        return _FakePurchased(kwargs["phone_number"], self._client.next_sid)

    def __call__(self, sid):
        return _IncomingCtx(self._client, sid)


class FakeTwilioClient:
    def __init__(self, pool, next_sid="PNfakesid0001"):
        # pool: {area_code_or_None: [phone_str, ...]}
        self.pool = pool
        self.next_sid = next_sid
        self.calls = []
        self.incoming_phone_numbers = _IncomingNumbers(self)
        self.messaging = _Messaging(self)

    def available_phone_numbers(self, country):
        self.calls.append(("available_phone_numbers", country))
        return _Available(self, country)

    # helpers for assertions
    def call_names(self):
        return [c[0] for c in self.calls]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

WEBHOOK = "https://app.example.com/twilio/webhook?slug=alice"
MSG_SVC = "MGxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"


def test_happy_path_returns_number_and_sid():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    number, sid = tn.provision_number(
        client, webhook_url=WEBHOOK, messaging_service_sid=MSG_SVC
    )
    assert number == "+15550001111"
    assert sid == "PNfakesid0001"


def test_call_order_is_search_then_buy_then_attach():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    tn.provision_number(client, webhook_url=WEBHOOK, messaging_service_sid=MSG_SVC)
    names = client.call_names()
    # search happens, then purchase, then attach — in that order.
    assert names.index("local.list") < names.index("incoming.create")
    assert names.index("incoming.create") < names.index("svc.attach")


def test_webhook_url_is_passed_to_purchase():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    tn.provision_number(client, webhook_url=WEBHOOK, messaging_service_sid=MSG_SVC)
    create_call = next(c for c in client.calls if c[0] == "incoming.create")
    kwargs = create_call[1]
    assert kwargs["sms_url"] == WEBHOOK
    assert "slug=alice" in kwargs["sms_url"]
    assert kwargs["sms_method"] == "POST"


def test_number_attached_to_messaging_service():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    _, sid = tn.provision_number(
        client, webhook_url=WEBHOOK, messaging_service_sid=MSG_SVC
    )
    attach = next(c for c in client.calls if c[0] == "svc.attach")
    assert attach[1] == MSG_SVC
    assert attach[2]["phone_number_sid"] == sid


def test_no_messaging_service_skips_attach_but_still_buys():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    number, _ = tn.provision_number(
        client, webhook_url=WEBHOOK, messaging_service_sid=None
    )
    assert number == "+15550001111"
    assert "svc.attach" not in client.call_names()
    assert "incoming.create" in client.call_names()


def test_empty_pool_raises():
    client = FakeTwilioClient(pool={None: []})
    with pytest.raises(tn.NoNumbersAvailableError):
        tn.provision_number(client, webhook_url=WEBHOOK, messaging_service_sid=MSG_SVC)
    # Must NOT have purchased anything.
    assert "incoming.create" not in client.call_names()


def test_area_code_fallback_searches_twice_then_buys():
    # Requested area code 212 has nothing; generic pool has a number.
    client = FakeTwilioClient(pool={"212": [], None: ["+15559998888"]})
    number, _ = tn.provision_number(
        client,
        webhook_url=WEBHOOK,
        messaging_service_sid=MSG_SVC,
        area_code="212",
    )
    assert number == "+15559998888"
    list_calls = [c for c in client.calls if c[0] == "local.list"]
    assert len(list_calls) == 2
    # first search constrained to the area code, second falls back (no area_code)
    assert list_calls[0][1].get("area_code") == "212"
    assert "area_code" not in list_calls[1][1]


def test_release_number_calls_delete():
    client = FakeTwilioClient(pool={None: []})
    tn.release_number(client, "PNtoRelease")
    assert ("incoming.delete", "PNtoRelease") in client.calls


def test_search_passes_sms_enabled_and_limit():
    client = FakeTwilioClient(pool={None: ["+15550001111"]})
    tn.search_available_numbers(client, area_code="415", limit=7)
    call = next(c for c in client.calls if c[0] == "local.list")
    assert call[1]["sms_enabled"] is True
    assert call[1]["limit"] == 7
    assert call[1]["area_code"] == "415"
