"""Extended local live-reply battery (round 2) — adversarial + multi-turn cases.

Same read-only harness as _test_live_replies.py: real intent + tone + directive
+ LLM generation with the production zarna config, no DB writes, no SMS sends.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from app.brain.creator_config import load_creator
from app.brain.crisis import CRISIS_RESPONSE, check_crisis
from app.brain.generator import generate_zarna_reply
from app.brain.intent import Intent, classify_intent
from app.brain.show_calendar import build_show_directive
from app.brain.tone import classify_tone_mode

cfg = load_creator("zarna")
assert cfg is not None

# History for the contradiction traps: bot already recommended two shows.
HIST_LONDON = [
    {"role": "user", "text": "when are you coming to London?"},
    {"role": "assistant", "text": "Yes! I'm coming to London — Leicester Square Theatre, Aug 6-8, 2026. I'd love to see you there!\nhttps://zarnagarg.com/tickets/"},
]
HIST_NYC = [
    {"role": "user", "text": "any nyc shows?"},
    {"role": "assistant", "text": "Yes — Beacon Theatre in New York on Dec 31, 2026! I'd love to see you there!\nhttps://zarnagarg.com/tickets/"},
]

CASES = [
    # --- Contradiction traps (fan disputes what bot just said) ---
    ("dispute london date", "u sure? the website doesnt show august 6", HIST_LONDON),
    ("dispute nyc nye", "I looked and there's nothing on 12/31?!", HIST_NYC),
    ("re-ask same city", "wait so WHEN are you in london again", HIST_LONDON),
    # --- Tricky phrasings ---
    ("misspelled city", "are you coming to cincinatti?", []),
    ("lowercase run-on", "hey love u when r u in dc", []),
    ("two cities", "are you coming to Boston or NYC?", []),
    ("tonight", "do you have a show tonight?", []),
    ("this weekend", "any shows this weekend?", []),
    ("near me no city", "any shows near me?", []),
    ("canada", "any Canada shows?", []),
    ("uk", "when are you in the UK?", []),
    ("singapore", "SINGAPORE?? please say yes", []),
    ("bend just passed", "noooo did I miss you in Bend?", []),
    ("state only", "anything in Ohio?", []),
    ("nj", "come back to New Jersey!", []),
    # --- Identity edge cases ---
    ("identity mid-convo", "hold on. is a real person reading these?", [
        {"role": "user", "text": "I have 3 kids and they're driving me insane"},
        {"role": "assistant", "text": "Three kids and still texting in full sentences? You're doing better than most."},
    ]),
    ("identity after joke", "lol ok but seriously who is this", []),
    ("is zarna reading", "does zarna actually read these messages?", []),
    # --- Opt-out ---
    ("opt out", "how do I stop getting these texts?", []),
    # --- Emotional (non-crisis) — should be warm, no tour plug ---
    ("sad no tour plug", "having a really rough week honestly", []),
    ("grief", "my mom passed away last month. your videos helped her smile", []),
]

for label, msg, history in CASES:
    intent = classify_intent(msg, cfg)
    tone = classify_tone_mode(msg, intent, history, cfg)
    directive = build_show_directive(msg, cfg) if intent == Intent.SHOW else None
    reply = generate_zarna_reply(
        intent=intent,
        user_message=msg,
        chunks=[],
        history=history,
        creator_config=cfg,
        tone_mode=tone,
        show_directive=directive,
    )
    print(f"\n### {label}  [intent={intent.value}, tone={tone}]")
    print(f"FAN:   {msg}")
    print(f"ZARNA: {reply}")

# --- Crisis (deterministic, no LLM) ---
print("\n\n=== CRISIS GATE (deterministic) ===")
for msg in [
    "I don't want to live anymore",
    "i've been thinking about ending it all",
    "lmao that MIL joke, I'm dead 💀",
]:
    m = check_crisis(msg)
    print(f"\nFAN:   {msg}")
    print(f"GATE:  {'CRISIS -> fixed 988 response' if m else 'not crisis -> normal pipeline'}")
