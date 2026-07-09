"""Local test battery: show-calendar directives for realistic fan messages.

Read-only. Prints the directive instruction the LLM would receive for each
message, so we can verify dates/cities are correct BEFORE testing live replies.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("GEMINI_API_KEY", "placeholder")

from app.brain.creator_config import load_creator
from app.brain import show_calendar as sc

cfg = load_creator("zarna")
assert cfg is not None, "failed to load zarna config"

cal = sc.get_calendar(cfg)
print(f"=== Calendar: {len(cal.upcoming)} upcoming, {len(cal.recent_past)} recent past ===")
for s in cal.upcoming:
    print(f"  UPCOMING  {s.date_label:18s} {s.city}, {s.region} — {s.venue}")
for s in cal.recent_past:
    print(f"  PAST      {s.date_label:18s} {s.city}, {s.region} — {s.venue}")

MESSAGES = [
    # The exact contradiction cases from the logs
    "I dont see tickets for dec4 2026",
    "When is the next time you're coming back to the New York City area?",
    "are you coming to portland?",
    "are you coming to Portland Maine?",
    "are you coming back to portland oregon?",
    # City with an upcoming show
    "when are you coming to London?",
    "I heard you are coming to london",
    "any shows in Dublin?",
    "Are you coming to Nashville?",
    "when are you in SF?",
    "Sydney show dates?",
    "coming to Hong Kong?",
    "any shows in paris",
    "are you coming to Halifax",
    "tickets for Amsterdam?",
    "when is the Ridgefield show",
    "westhampton beach tickets?",
    # City with NO show
    "when are you coming to Phoenix?",
    "Come to the Lehigh Valley Pennsylvania sometime!",
    "any shows in Colorado?",
    "are you coming to Delaware?",
    # Date-based questions
    "do you have a show on December 4?",
    "is there a show on 12/31?",
    "anything on Aug 30?",
    "tickets for the July 10 show?",
    # Region questions
    "any shows in Europe?",
    "are you coming to Asia?",
    "any Australia dates?",
    # Recent past
    "are you coming back to Bend?",
    "when are you back in Austin?",
    # Generic
    "what's your next show?",
    "where can I buy tickets?",
]

print("\n=== Directives ===")
for msg in MESSAGES:
    d = sc.build_show_directive(msg, cfg)
    print(f"\nFAN: {msg}")
    if d is None:
        print("  -> None (generic ticket reply)")
    else:
        print(f"  -> link: {d.ticket_url}")
        print(f"  -> {d.instruction}")
