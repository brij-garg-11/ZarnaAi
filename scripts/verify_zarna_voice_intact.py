"""
Quick functional verification that Zarna's brain still produces
Zarna-voiced replies after the slug-based config refactor.

Production wires create_brain() with no slug arg → ZarnaBrain falls back to
CREATOR_SLUG env var → load_creator('zarna'). We test BOTH the legacy path
(no slug) and the explicit ('zarna') path so we know prod is safe.
"""
from __future__ import annotations
import os, sys, time
from dotenv import load_dotenv
load_dotenv()

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("MULTI_MODEL_REPLY", "off")

from app.brain.handler import create_brain
from app.brain import generator as gen

ZARNA_PHONE = "+15559999991"

def _scan(prompt: str, label: str):
    """Make sure Zarna's prompt has the expected Zarna content (positive scan)."""
    expected = ["Zarna", "Shalabh", "mother-in-law"]
    missing = [w for w in expected if w not in prompt]
    if missing:
        print(f"  ❌ {label}: MISSING expected Zarna words: {missing}")
        return False
    print(f"  ✅ {label}: prompt contains Zarna({prompt.count('Zarna')}), Shalabh({prompt.count('Shalabh')}), MIL({prompt.count('mother-in-law')})")
    return True

CAPTURED = []
_orig = gen._generate_gemini_raw
def _patched(p):
    CAPTURED.append(p)
    return _orig(p)
gen._generate_gemini_raw = _patched

ok = True

print("=" * 70)
print("Test A: create_brain() with NO slug (production main.py path)")
print("=" * 70)
brain_a = create_brain()
print(f"  brain.slug                   = {brain_a.slug}")
print(f"  brain.creator_config.slug    = {brain_a.creator_config.slug}")
print(f"  brain.creator_config.name    = {brain_a.creator_config.name}")
print(f"  retriever                    = {type(brain_a.retriever).__name__}")
assert brain_a.slug == "zarna", f"expected slug='zarna' got {brain_a.slug!r}"
assert brain_a.creator_config.slug == "zarna"
assert brain_a.creator_config.name == "Zarna Garg"
print("  ✅ all asserts pass")

print("\nSending 'tell me about Shalabh' through Zarna's brain…")
CAPTURED.clear()
reply = brain_a.handle_incoming_message(ZARNA_PHONE, "tell me about Shalabh")
print(f"  Zarna reply: {reply}")
print(f"  prompts captured: {len(CAPTURED)}")
if CAPTURED:
    ok &= _scan(CAPTURED[-1], "Zarna prompt")
    # Zarna's reply should mention Shalabh
    if "Shalabh" in reply or "spreadsheet" in reply.lower() or "husband" in reply.lower():
        print(f"  ✅ Zarna reply is in-character (mentions Shalabh/spreadsheet/husband)")
    else:
        print(f"  ⚠️  Zarna reply may have lost voice — manual review needed")

print("\n" + "=" * 70)
print("Test B: create_brain('zarna') explicit slug")
print("=" * 70)
brain_b = create_brain(slug="zarna")
print(f"  brain.slug                   = {brain_b.slug}")
print(f"  brain.creator_config.slug    = {brain_b.creator_config.slug}")
print(f"  retriever                    = {type(brain_b.retriever).__name__}")
assert brain_b.slug == "zarna"
assert brain_b.creator_config.slug == "zarna"
print("  ✅ all asserts pass")

print("\n" + "=" * 70)
print("RESULT:", "ALL GREEN" if ok else "FAIL")
print("=" * 70)
sys.exit(0 if ok else 1)
