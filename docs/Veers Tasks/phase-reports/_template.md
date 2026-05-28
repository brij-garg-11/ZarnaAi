# Phase NN report: <Phase title>

**Date:** YYYY-MM-DD
**Tester:** Veer
**Time spent:** ~Xh
**Status:** ✅ Clean / ⚠️ Mostly clean (small notes) / ❌ Blockers found

---

## Summary

2–3 sentences. Did it work? Anything weird? If you found a critical bug, lead with it.

---

## Tests run

| Test ID | Result | Notes |
|---|---|---|
| XXX-01 | ✅ Pass | |
| XXX-02 | ⚠️ Pass with note | <one-line observation> |
| XXX-03 | ❌ Fail | <why it failed> — see fix PR #N |
| XXX-04 | ⏭️ Skipped | <why> (e.g. needs prod access I don't have) |

**Result legend:** ✅ pass · ⚠️ passes but worth noting · ❌ fail · ⏭️ skipped (with reason)

---

## Bugs found

| # | Severity | Bug | Fix PR | Status |
|---|---|---|---|---|
| 1 | Medium | <one-liner> | [#N](link) | Merged / open / flagged for Brij |

If no bugs: write "None."

---

## Code changes I made

- <staging-only changes I committed in THIS phase report PR>
- <prod changes I sent in a SEPARATE PR to `main`: [#N](link)>

If no code changes: write "Phase report only."

---

## What I noticed (not bugs, just things)

- <UX observations, slow page loads, confusing copy, etc.>
- <"this is brittle" notes for future hardening>

If nothing: skip this section.

---

## Flagged for Brij

Anything I didn't fix because the playbook's "DO NOT TOUCH" rules said to ask first:

- <one-liner with where to find it>

If nothing: skip this section.

---

## Ready for next phase?

- [ ] Yes
- [ ] No (blocker: <what's blocking>)

---

## When opening the PR

Copy this into the PR description:

```
**Status:** ✅ / ⚠️ / ❌
**Time spent:** ~Xh

**Summary:**
<2–3 sentences>

**Bugs found:**
| # | Severity | Bug | Fix PR |
|---|---|---|---|
| 1 | ... | ... | #N |

(or "None.")

Full report: docs/Veers Tasks/phase-reports/PNN-<title>.md
```
