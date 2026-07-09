"""Local live-reply battery: end-to-end intent -> directive -> LLM reply.

Calls the real generation pipeline (Gemini/OpenAI/Anthropic per routing) with
the production zarna config, but NO database and NO messaging — read-only
against the outside world except the LLM APIs. Run:  python3 scripts/_test_live_replies.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Load .env exactly like main.py does.
from dotenv import load_dotenv
load_dotenv()

from app.brain.creator_config import load_creator
from app.brain.generator import generate_zarna_reply
from app.brain.intent import Intent, classify_intent
from app.brain.show_calendar import build_show_directive
from app.brain.tone import classify_tone_mode

cfg = load_creator("zarna")
assert cfg is not None

# (label, message, history)
CASES = [
    # --- Tour dates: the "diamond good" cases ---
    ("dec4 dispute (with history)", "I dont see tickets for dec4 2026", [
        {"role": "user", "text": "When is the next time you're coming to Portland Maine?"},
        {"role": "assistant", "text": "Yes! I'm coming to Portland — State Theatre on Dec 4, 2026. I'd love to see you there!\nhttps://zarnagarg.com/tickets/"},
    ]),
    ("nyc area", "When is the next time you're coming back to the New York City area?", []),
    ("bare portland", "are you coming to portland?", []),
    ("portland oregon (just passed)", "are you coming back to portland oregon?", []),
    ("london", "when are you coming to London?", []),
    ("dublin", "any shows in Dublin?", []),
    ("nashville", "Are you coming to Nashville?", []),
    ("phoenix (no show)", "when are you coming to Phoenix?", []),
    ("europe", "any shows in Europe?", []),
    ("australia", "any Australia dates?", []),
    ("date no show", "do you have a show on August 30?", []),
    ("next show", "what's your next show?", []),
    ("generic tickets", "where can I buy tickets?", []),

    # --- AI identity ---
    ("who is this", "Who is this?", []),
    ("is this really zarna", "Is this really Zarna texting me?", []),
    ("are you a bot", "are you a bot?", []),
    ("real person", "am I talking to a real person?", []),
    ("who am i texting", "wait who am I texting right now", []),
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
    print(f"\n### {label}  [intent={intent.value}]")
    print(f"FAN:   {msg}")
    print(f"ZARNA: {reply}")
