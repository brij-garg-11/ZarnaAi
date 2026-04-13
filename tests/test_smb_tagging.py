"""
Tests for app/smb/tagging.py

Covers:
- infer_geo: NYC numbers → LOCAL, non-NYC → OUT_OF_TOWN, international → OUT_OF_TOWN
- tag_geo: correct preference saved to DB
- tag_engagement_async: HIGH engagement after threshold, ticket/deal intent detection
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock, call

from app.smb.tagging import (
    infer_geo,
    tag_geo,
    tag_engagement_async,
    _HIGH_ENGAGEMENT_THRESHOLD,
)


# ---------------------------------------------------------------------------
# infer_geo — pure function
# ---------------------------------------------------------------------------

def test_nyc_borough_numbers_are_local():
    assert infer_geo("+12125551234") == "LOCAL"   # 212 Manhattan
    assert infer_geo("+17185551234") == "LOCAL"   # 718 Brooklyn/Queens/Bronx/SI
    assert infer_geo("+16465551234") == "LOCAL"   # 646 Manhattan overlay
    assert infer_geo("+19175551234") == "LOCAL"   # 917 NYC mobile
    assert infer_geo("+13475551234") == "LOCAL"   # 347 Brooklyn/Queens overlay
    assert infer_geo("+19295551234") == "LOCAL"   # 929 NYC overlay
    print("✓ all NYC borough area codes → LOCAL")


def test_nj_commuter_numbers_are_local():
    assert infer_geo("+12015551234") == "LOCAL"   # 201 NJ Hudson County
    assert infer_geo("+19735551234") == "LOCAL"   # 973 NJ Essex/Passaic
    assert infer_geo("+17325551234") == "LOCAL"   # 732 NJ Middlesex
    print("✓ NJ commuter belt area codes → LOCAL")


def test_suburban_ny_numbers_are_local():
    assert infer_geo("+19145551234") == "LOCAL"   # 914 Westchester
    assert infer_geo("+15165551234") == "LOCAL"   # 516 Long Island Nassau
    assert infer_geo("+16315551234") == "LOCAL"   # 631 Long Island Suffolk
    print("✓ suburban NY area codes → LOCAL")


def test_non_nyc_us_numbers_are_out_of_town():
    assert infer_geo("+13105551234") == "OUT_OF_TOWN"  # 310 LA
    assert infer_geo("+17735551234") == "OUT_OF_TOWN"  # 773 Chicago
    assert infer_geo("+14155551234") == "OUT_OF_TOWN"  # 415 SF
    assert infer_geo("+13055551234") == "OUT_OF_TOWN"  # 305 Miami
    print("✓ non-NYC US area codes → OUT_OF_TOWN")


def test_international_numbers_are_out_of_town():
    assert infer_geo("+447911123456") == "OUT_OF_TOWN"  # UK
    assert infer_geo("+525512345678") == "OUT_OF_TOWN"  # Mexico
    assert infer_geo("+61412345678") == "OUT_OF_TOWN"   # Australia
    print("✓ international numbers → OUT_OF_TOWN")


def test_number_without_country_code():
    assert infer_geo("2125551234") == "LOCAL"       # 212 without +1
    assert infer_geo("3105551234") == "OUT_OF_TOWN" # 310 without +1
    print("✓ numbers without +1 country code handled correctly")


def test_empty_or_malformed_returns_out_of_town():
    assert infer_geo("") == "OUT_OF_TOWN"
    assert infer_geo(None) == "OUT_OF_TOWN"
    assert infer_geo("abc") == "OUT_OF_TOWN"
    print("✓ empty/malformed numbers → OUT_OF_TOWN (safe default)")


# ---------------------------------------------------------------------------
# tag_geo — calls save_preference with correct value
# ---------------------------------------------------------------------------

def test_tag_geo_saves_local():
    mock_conn = MagicMock()
    with patch("app.smb.tagging.smb_storage") as mock_storage:
        tag_geo(mock_conn, subscriber_id=42, phone_number="+12125551234")
    mock_storage.save_preference.assert_called_once_with(mock_conn, 42, "geo", "LOCAL")
    print("✓ tag_geo saves LOCAL for NYC number")


def test_tag_geo_saves_out_of_town():
    mock_conn = MagicMock()
    with patch("app.smb.tagging.smb_storage") as mock_storage:
        tag_geo(mock_conn, subscriber_id=7, phone_number="+13105551234")
    mock_storage.save_preference.assert_called_once_with(mock_conn, 7, "geo", "OUT_OF_TOWN")
    print("✓ tag_geo saves OUT_OF_TOWN for non-NYC number")


# ---------------------------------------------------------------------------
# tag_engagement_async — engagement threshold + intent detection
# ---------------------------------------------------------------------------

def _run_tag(phone="+12125550001", sub_id=1, slug="test", message="hey", count=0):
    """Helper: call tag_engagement_async with a mock DB connection."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    with patch("app.smb.tagging.get_db_connection", return_value=mock_conn):
        with patch("app.smb.tagging.smb_storage") as mock_storage:
            tag_engagement_async(phone, sub_id, slug, message, count)
            return mock_storage.save_preference.call_args_list


def test_below_threshold_no_engagement_tag():
    calls = _run_tag(count=_HIGH_ENGAGEMENT_THRESHOLD - 1, message="hi")
    keys_saved = [c[0][2] for c in calls]  # 3rd positional arg = question_key
    assert "engagement" not in keys_saved
    print("✓ below threshold: no engagement tag saved")


def test_at_threshold_saves_high_engagement():
    calls = _run_tag(count=_HIGH_ENGAGEMENT_THRESHOLD, message="hi")
    keys_saved = [c[0][2] for c in calls]
    assert "engagement" in keys_saved
    engagement_call = next(c for c in calls if c[0][2] == "engagement")
    assert engagement_call[0][3] == "HIGH"
    print("✓ at threshold: engagement=HIGH saved")


def test_above_threshold_saves_high_engagement():
    calls = _run_tag(count=_HIGH_ENGAGEMENT_THRESHOLD + 5, message="hi")
    keys_saved = [c[0][2] for c in calls]
    assert "engagement" in keys_saved
    print("✓ above threshold: engagement=HIGH saved")


def test_ticket_intent_detected():
    calls = _run_tag(message="Can I still get tickets for tonight?")
    keys_saved = [c[0][2] for c in calls]
    assert "intent_tickets" in keys_saved
    ticket_call = next(c for c in calls if c[0][2] == "intent_tickets")
    assert ticket_call[0][3] == "YES"
    print("✓ ticket intent detected in message")


def test_deal_intent_detected():
    calls = _run_tag(message="Do you have any discounts or deals tonight?")
    keys_saved = [c[0][2] for c in calls]
    assert "intent_deals" in keys_saved
    deal_call = next(c for c in calls if c[0][2] == "intent_deals")
    assert deal_call[0][3] == "YES"
    print("✓ deal intent detected in message")


def test_no_intent_signals_nothing_saved():
    calls = _run_tag(count=0, message="haha that was funny")
    keys_saved = [c[0][2] for c in calls]
    assert "intent_tickets" not in keys_saved
    assert "intent_deals" not in keys_saved
    assert "engagement" not in keys_saved
    print("✓ neutral message: no intent tags saved")


def test_multiple_intents_in_one_message():
    calls = _run_tag(
        count=_HIGH_ENGAGEMENT_THRESHOLD,
        message="Are there any discount tickets available for tonight's show?",
    )
    keys_saved = [c[0][2] for c in calls]
    assert "engagement" in keys_saved
    assert "intent_tickets" in keys_saved
    assert "intent_deals" in keys_saved
    print("✓ multiple intents detected in single message")


def test_no_db_connection_does_not_raise():
    with patch("app.smb.tagging.get_db_connection", return_value=None):
        tag_engagement_async("+12125550001", 1, "test", "tickets please", 5)
    print("✓ no DB connection handled gracefully (no exception)")


def test_db_exception_does_not_raise():
    mock_conn = MagicMock()
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    with patch("app.smb.tagging.get_db_connection", return_value=mock_conn):
        with patch("app.smb.tagging.smb_storage") as mock_storage:
            mock_storage.save_preference.side_effect = Exception("DB error")
            tag_engagement_async("+12125550001", 1, "test", "tickets please", 5)
    print("✓ DB exception handled gracefully (no exception propagated)")
