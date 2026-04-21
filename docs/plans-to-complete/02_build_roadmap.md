# Plan: Build Roadmap — Pillars 1–4
_Source: `docs/Brijs Thoughts/build_roadmap.md`_

Pillar 5 (Smart Blast Engine) is shipped. Remaining steps for Pillars 1–4:

## Pillar 2 — Link Clicked Within 1h

- [ ] **Step 1** — Verify `link_clicked_1h` wiring end-to-end
  - Column exists in DB, operator blast tool already flips it on click
  - Test: send blast with tracked link → click link → verify `link_clicked_1h = true` on correct message row within 1 hour

## Pillar 1 — Self-Improvement Loop

- [ ] **Step 2** — Extract silence scoring into its own Railway cron (`scripts/score_silence.py`)
  - Currently runs inline; extract to independent job
  - Test: verify cron runs independently, scores update in DB, main reply pipeline unaffected

- [ ] **Step 3** — Cold-start fix for winning examples
  - Seed corpus on first deploy from top N highest-scoring historical replies
  - Store version snapshot for rollback
  - Test: run quality digest before/after — verify no regression

## Pillar 3 — Context-Aware Selling

- [ ] **Step 6** — Winning examples expansion + rollback
  - Expand corpus quarterly with new material
  - Version each snapshot with a date; keep prior snapshot for one-command rollback
  - Test: run quality digest before/after; if scores drop, roll back and verify recovery

## Pillar 4 — Smart Audience Segmentation

- [ ] **Step 8** — Compound segment builder
  - AND/OR filter logic in blast audience picker
  - Test: build compound filter → verify fan count matches manual DB query

- [ ] **Step 9** — Random % within segment
  - Allow X% sample of filtered audience
  - Test: set 20% on known segment → verify recipient count matches ~20%

- [ ] **Step 10** — UX control for frequency
  - Expose `fan_tier` and `last_blasted_at` in admin audience view
  - Test: open audience view → verify tier + last-blasted-at columns display correctly
