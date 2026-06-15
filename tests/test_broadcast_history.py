"""
Tests for live-show broadcast persistence.

A live-show broadcast must be saved to the shared conversation history so it
appears in the inbox thread (giving a fan's later reply preceding context) and
the recipient shows in the inbox list. run_loop_broadcast surfaces each
successful send via the on_success hook, which the worker uses to persist.
"""

from app.messaging.broadcast import run_loop_broadcast


def _slick_ok(_to, _body):
    return True


def _slick_fail(_to, _body):
    return False


def test_on_success_called_only_for_successful_sends():
    sent = []
    res = run_loop_broadcast(
        phones=["+15551112222", "+15553334444"],
        body="Live in 10!",
        provider="slicktext",
        deliver_whatsapp=False,
        slicktext_send=_slick_ok,
        on_success=lambda p: sent.append(p),
    )
    assert res.succeeded == 2
    assert res.failed == 0
    assert sent == ["+15551112222", "+15553334444"]


def test_on_success_skipped_for_failed_sends():
    sent = []
    res = run_loop_broadcast(
        phones=["+15551112222"],
        body="Live in 10!",
        provider="slicktext",
        deliver_whatsapp=False,
        slicktext_send=_slick_fail,
        on_success=lambda p: sent.append(p),
    )
    assert res.succeeded == 0
    assert res.failed == 1
    assert sent == []


def test_on_success_hook_exception_does_not_break_send_loop():
    def _boom(_p):
        raise RuntimeError("persistence is down")

    res = run_loop_broadcast(
        phones=["+15551112222", "+15553334444"],
        body="Live in 10!",
        provider="slicktext",
        deliver_whatsapp=False,
        slicktext_send=_slick_ok,
        on_success=_boom,
    )
    # Sending must still complete even when persistence raises.
    assert res.succeeded == 2
    assert res.failed == 0


def test_broadcast_works_without_on_success_hook():
    res = run_loop_broadcast(
        phones=["+15551112222"],
        body="Live in 10!",
        provider="slicktext",
        deliver_whatsapp=False,
        slicktext_send=_slick_ok,
    )
    assert res.succeeded == 1
