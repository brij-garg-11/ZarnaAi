import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.intent import Intent
from app.brain.tone import classify_tone_mode


def test_tone_sensitive_care_for_vulnerability():
    mode = classify_tone_mode("I feel really anxious and overwhelmed", Intent.GENERAL, [])
    assert mode == "sensitive_care"


def test_tone_roast_for_shalabh_topic():
    mode = classify_tone_mode("What do you think about Shalabh?", Intent.GENERAL, [])
    assert mode == "roast_playful"


def test_tone_celebratory_for_show_praise():
    mode = classify_tone_mode("Awesome show tonight, you killed it!", Intent.GENERAL, [])
    assert mode == "celebratory"


def test_tone_direct_answer_for_plain_question():
    mode = classify_tone_mode("Where can I watch your clips?", Intent.GENERAL, [])
    assert mode == "direct_answer"
