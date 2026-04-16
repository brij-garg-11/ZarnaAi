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

**Step 4:** Merch intent. ✅ SHIPPED
- `MERCH` added as a first-class intent in `app/brain/intent.py`.
- Conservative two-signal keyword detection: requires both a merch item word (shirt, hoodie, merch, etc.) AND a purchase/query word (buy, shop, where, etc.). "Your shirt is hilarious" → FEEDBACK; "where can I buy your shirt?" → MERCH.
- Extra phrase list catches "do you have merch?"-style questions.
- MERCH prompt in `app/brain/generator.py` outputs 1 sentence + `https://shopmy.us/shop/zarnagarg` link.
- MERCH added to `_STRUCTURED_INTENTS` (Gemini-only, link fidelity) and `_STRUCTURED_ROUTE_INTENTS` (no complexity router call).
- **Test:** `tests/test_context_aware_selling.py` — 26 tests cover positives, false-positive guards, prompt link injection, handler routing flags. All pass.

**Step 5:** Per-show / per-city sell copy. ✅ SHIPPED
- `get_fan_location(phone_number)` added to `BaseStorage`, `PostgresStorage`, `InMemoryStorage`.
- `get_fan_show_context(phone_number)` added to `BaseStorage` and `PostgresStorage` — queries `smb_show_checkins` (door check-in) then `live_show_signups` (pre-signup); returns human-readable string.
- Handler (`app/brain/handler.py`) assembles `sell_context` for SHOW and MERCH intents and passes it to the generator.
- Generator injects context into the prompt: "Fan attended 'Chicago Laugh Factory' on 2025-03-15." → bot can say "You're a true Chicago fan — here's where to grab tickets."
- **Test:** `TestSellContextStorage` in `tests/test_context_aware_selling.py` — location storage and context assembly verified.

**Step 6:** More winning examples with rollback.
- Expand the winning-examples corpus systematically (new material added quarterly).
- Version each corpus snapshot with a date. Keep the prior snapshot available for one-command rollback.
- **Test:** Run quality digest before and after. If scores drop, roll back and verify recovery.

**Step 7:** A/B testing on sell copy. ✅ SHIPPED
- Handler randomly assigns `sell_variant = "A"` or `"B"` for every SHOW or MERCH reply.
- Variant B gets a different tone instruction (warm + personal) in the generator prompt.
- `sell_variant` column added to `messages` table via migration.
- `sell_variant` stored via `save_reply_context` and logged to DB asynchronously.
- **Test:** `TestSellVariantAssignment` in `tests/test_context_aware_selling.py` — distribution, storage, and non-sell intent (None) all verified.
- **Next:** expose variant vs. reply rate in admin quality tab.

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
