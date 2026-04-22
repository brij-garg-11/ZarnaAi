# Plan: Build Roadmap — Pillars 1–4
_Source: `docs/Brijs Thoughts/build_roadmap.md`_

## Already Done ✅

- **Step 1** (Pillar 2) — `link_clicked_1h` is fully wired in both the main app (`app/admin/__init__.py`) and operator blast tool. DB column exists. No action needed.
- **Step 2** (Pillar 1) — "score silence nightly" service is live on Railway with `cronSchedule = "0 2 * * *"` and `operator/railway.score_silence.toml`. Running every night.
- **Step 3** (Pillar 1) — Cold-start seed run on Apr 21, 2026. 200 examples inserted under snapshot `2026-04-21` for creator `zarna`. Rollback: `python scripts/seed_winning_examples.py --creator zarna --rollback 2026-04-21`
- **Step 4** (Pillar 3) — MERCH intent. ✅ SHIPPED
- **Step 5** (Pillar 3) — Per-show / per-city sell copy. ✅ SHIPPED
- **Step 7** (Pillar 3) — A/B testing on sell copy. ✅ SHIPPED
- **Step 9** (Pillar 4) — Random % within segment already live in blast UI and `get_audience_phones()`.

---

## Still To Build

### Step 6 — Winning Examples Expansion (Pillar 3)
_Operational process — no new code needed. Run quarterly._
- [ ] Run `python scripts/seed_winning_examples.py --tag YYYY-MM-DD` each quarter to expand the corpus
- [ ] Prior snapshot stays available for rollback if quality drops
- [ ] Next run due: ~Jul 2026

### Step 8 — Compound Segment Builder (Pillar 4)
_Real new work — backend + UI_
- [ ] Add AND/OR filter logic to `get_audience_phones()` in `operator/app/queries.py`
- [ ] Update blast audience picker UI in `operator/app/templates/blast.html`
- [ ] Example: fans tagged `longtime-fan` AND in `New York` AND signed up for a live show
- [ ] Test: compound filter fan count matches manual DB query

### Step 10 — Frequency UX in Audience View (Pillar 4)
_Builds on fan tiers (already running)_
- [ ] Add `fan_tier` and `last_blasted_at` columns to the audience view in `operator/app/templates/audience.html`
- [ ] Backend query to pull tier + last blast date per fan
- [ ] Test: open audience view → verify columns display correctly
