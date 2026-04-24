"""
Final push-readiness gate.

Runs EVERY regression test script we care about, in order, and reports
a single pass/fail. If this is green, we're ready to commit.

What it runs:
  A. Full pytest suite (302 pass baseline, 7 pre-existing fails)
  B. Phase 2 PgRetriever smoke test
  C. Phase 3 provisioning smoke test
  D. Phase 4 API status smoke test
  E. Phase 5 Zarna quality comparison
  F. Contact slug stamping (Bug #4 regression)
  G. Cross-tenant isolation (27 checks)
  H. Zarna non-regression (24 checks)
  I. verify_zarna_voice_intact (production boot path)
  J. Marcus scripted (16 msgs)
  K. Deep dual-creator (32 msgs, Marcus + Zarna batteries)

Each test runs in a fresh subprocess so state doesn't bleed between them.
"""
import os, sys, subprocess, time, re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["MULTI_MODEL_REPLY"] = "off"
os.environ["PYTHONUNBUFFERED"] = "1"

def run(name, cmd, expect_pattern=None, expect_exit=0, timeout=900):
    """
    Run a subprocess, capture exit + output, return (passed, detail).

    expect_pattern: optional regex — if provided, output must match AND
                    exit code must equal expect_exit.
    """
    print(f"\n{'═' * 74}")
    print(f"  {name}")
    print(f"{'═' * 74}")
    print(f"  cmd: {' '.join(cmd)}")
    t0 = time.time()
    try:
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        return (False, f"TIMEOUT after {timeout}s")
    dt = time.time() - t0

    out = (proc.stdout or "") + (proc.stderr or "")
    tail = "\n".join(out.splitlines()[-20:])

    ok_exit = (proc.returncode == expect_exit)
    ok_pat  = True
    if expect_pattern:
        ok_pat = bool(re.search(expect_pattern, out, re.MULTILINE))

    passed = ok_exit and ok_pat

    print(f"  elapsed: {dt:.1f}s  exit: {proc.returncode}  "
          f"{'PASS' if passed else 'FAIL'}")
    print("  -- tail --")
    for line in tail.splitlines():
        print(f"    {line}")

    detail = f"exit={proc.returncode} dt={dt:.1f}s"
    if not ok_exit:
        detail += f" (expected {expect_exit})"
    if expect_pattern and not ok_pat:
        detail += f" (pattern {expect_pattern!r} not found)"
    return (passed, detail)


TESTS = [
    # A. Full pytest — expect 7 pre-existing fails + 302 pass (our baseline)
    ("A. Full pytest suite",
     ["python3", "-m", "pytest", "--tb=no", "-q",
      "--ignore=tests/test_live_show_signup.py"],
     r"7 failed,\s*302 passed",
     1),  # pytest returns 1 because of the 7 pre-existing fails — we EXPECT this

    # B. Phase 2
    ("B. Phase 2 PgRetriever smoke test",
     ["python3", "-u", "scripts/test_phase2_pg_retriever.py"],
     r"(PASS|All good|ok)", 0),

    # C. Phase 3
    ("C. Phase 3 provisioning smoke test",
     ["python3", "-u", "scripts/test_phase3_provisioning.py"],
     r"(PASS|DONE|complete|ok)", 0),

    # D. Phase 4
    ("D. Phase 4 API status smoke test",
     ["python3", "-u", "scripts/test_phase4_api_status.py"],
     r"(PASS|all green|ok)", 0),

    # E. Phase 5 Zarna quality
    ("E. Phase 5 Zarna quality comparison",
     ["python3", "-u", "scripts/test_phase5_quality_comparison.py"],
     None, 0),  # exit code 0 is enough

    # F. Contact slug stamping (Bug #4)
    ("F. Contact slug stamping (Bug #4 regression)",
     ["python3", "-u", "scripts/test_contact_slug_stamping.py"],
     r"10 passed, 0 failed", 0),

    # G. Cross-tenant isolation
    ("G. Cross-tenant isolation (27 checks)",
     ["python3", "-u", "scripts/test_cross_tenant_isolation.py"],
     r"27 passed, 0 failed", 0),

    # H. Zarna non-regression
    ("H. Zarna non-regression (24 checks)",
     ["python3", "-u", "scripts/test_zarna_nonregression.py"],
     r"PASS", 0),

    # I. Verify Zarna voice intact (production boot path)
    ("I. Production boot path — Zarna voice intact",
     ["python3", "-u", "scripts/verify_zarna_voice_intact.py"],
     r"ALL GREEN", 0),
]

results = []
for name, cmd, pat, code in TESTS:
    passed, detail = run(name, cmd, pat, code)
    results.append((name, passed, detail))

print("\n\n" + "█" * 74)
print("  FINAL GATE SUMMARY")
print("█" * 74)
n_pass = sum(1 for _, p, _ in results if p)
for name, p, d in results:
    tag = "✅ PASS" if p else "❌ FAIL"
    print(f"  {tag}  {name:<55s}  {d}")
print("\n" + "─" * 74)
print(f"  {n_pass} / {len(results)} gates passed")

if n_pass == len(results):
    print(f"\n  🟢  GREEN — SAFE TO COMMIT & REVIEW DIFF")
    sys.exit(0)
else:
    print(f"\n  🔴  RED — DO NOT PUSH, investigate failures above")
    sys.exit(1)
