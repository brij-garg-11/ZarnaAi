# Build Roadmap — Pillars 1–4

Pillar 5 (Smart Blast Engine) is being built first. This doc tracks the remaining work for Pillars 1–4. Pick up each pillar after Pillar 5 is shipped and tested.

---

## Pillar 2 — Link Clicked Within 1h

**Step 1:** Wire up `link_clicked_1h` tracking logic.
- `messages.link_clicked_1h` column already exists in the DB (currently 0 values everywhere).
- When a fan clicks a tracked link (`tracked_link_clicks`), look back at the most recent outbound message sent to that fan — if the click happened within 60 minutes of that message's `created_at`, flip `link_clicked_1h = true` on that message row.
- **Test:** Send a blast with a tracked link → click the link → verify `link_clicked_1h = true` on the correct message row within 1 hour.

---

## Pillar 1 — Self-Improvement Loop

**Step 2:** Separate silence-scoring cron.
- Silence scoring currently runs inline with other logic.
- Extract into its own Railway cron job (`scripts/score_silence.py` or similar).
- **Test:** Verify the cron runs independently, scores update in DB, and nothing in the main reply pipeline breaks.

**Step 3:** Cold-start fix for winning examples.
- Today the winning-examples corpus is sparse when a new creator launches — the bot has little to reference.
- Seed strategy: on first deploy, bootstrap the corpus from the top N highest-scoring historical replies (by quality digest score). Store a version snapshot so if a new batch degrades quality it can be rolled back to the prior snapshot.
- **Test:** Run quality digest before and after adding the seeded corpus. Verify no regression in reply scores.

---

## Pillar 3 — Context-Aware Selling

**Step 4:** Merch intent.
- Add `merch` as a first-class intent label in `app/brain/intent.py`.
- Route merch intent to a reply that surfaces the correct merch link.
- **Test:** Send a merch-related message ("where can I buy your shirt?") → verify `intent = merch` in the DB and the reply includes a merch link.

**Step 5:** Per-show / per-city sell copy.
- When the bot decides to sell, check if the fan has a show check-in (`smb_show_checkins` or `live_show_signups`) or a `fan_location` tag.
- Reference that context in the sell copy ("you were at the Chicago show…").
- **Test:** Simulate a fan with a show check-in → trigger a sell reply → verify the city/show name appears in the response.

**Step 6:** More winning examples with rollback.
- Expand the winning-examples corpus systematically (new material added quarterly).
- Version each corpus snapshot with a date. Keep the prior snapshot available for one-command rollback.
- **Test:** Run quality digest before and after. If scores drop, roll back and verify recovery.

**Step 7:** A/B testing on sell copy.
- When the bot routes to a sell reply (show, book, merch), randomly assign the fan to variant A or B.
- Log which variant was used on the message row.
- Track reply rate and link click rate per variant in the admin quality tab.
- **Test:** Run 20 test messages → verify ~50/50 split in DB → verify variant label is logged.

---

## Pillar 4 — Smart Audience Segmentation

**Step 8:** Compound segment builder.
- Add AND/OR filter logic to the blast audience picker in the operator UI.
- Example: fans tagged `longtime-fan` AND in `New York` AND signed up for a live show.
- **Test:** Build a compound filter → verify the returned fan count matches a manual DB query with the same conditions.

**Step 9:** Random % within segment.
- When sending to a large segment, allow the operator to enter an X% sample (e.g. 20%).
- System randomly selects that percentage of the filtered audience before sending.
- **Test:** Set 20% on a known segment size → verify actual recipient count matches ~20%.

**Step 10:** UX control for frequency (builds on Pillar 5 tiers).
- Once tiers are stored on contacts (Pillar 5 Step 3), expose `fan_tier` and `last_blasted_at` in the admin audience view.
- Operator can see each fan's tier and when they were last messaged before triggering any blast.
- **Test:** Open the audience view → verify tier and last-blasted-at columns display correctly for a sample of contacts.

---

*Start each step only after the prior one passes its test. Update status here as work progresses.*
