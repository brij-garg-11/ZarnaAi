# ZarnaBot Blast Issues — April 8, 2026

Issues discovered during and after the Malala X Zarna blast (draft id=61, sent to 3,651 contacts at ~12pm ET).

---

## 1. Blast button has no lock — triple send risk

**What happened:** The send button was clicked 3 times, creating 3 identical draft records (id=59, 60, 61) all with `status='draft'` and `sent_at=NULL`. One blast executed (id=61 got marked sent), but there was no UI feedback to confirm it worked, so the button appeared to do nothing.

**Fix needed:** Mark the draft as `status='sending'` immediately on first click and disable the button. Show a real-time confirmation once done ("Sent to X subscribers").

---

## 2. No post-send confirmation in the UI

**What happened:** After clicking send, the operator had no way to know the blast was running or completed without watching Railway logs. This caused the triple-click above.

**Fix needed:** After send completes, update the blast row in the UI to show sent count, timestamp, and a clear "Sent ✓" state.

---

## 3. Subscriber counts were wrong everywhere

**What happened:** Three different numbers, all wrong:
- SlickText dashboard showed **7,757** — inflated raw count across both textwords, includes duplicates
- Our admin dashboard showed **5,172** — only counts contacts who have ever texted our bot directly
- Real deduplicated active count from SlickText API: **5,025**

**Fix needed:** Admin dashboard "Total Subscribers" should pull from SlickText API and deduplicate across both textwords (`zarna` id=3185378, `hello` id=4633842).

---

## 4. Blast only reached 3,651 of 5,025 subscribers

**What happened:** Our blast pulls recipients from the internal `contacts` table (people who have directly texted our bot). The remaining ~1,374 subscribers joined via SlickText's native keyword and never texted back, so they're not in our DB and don't receive our blasts.

**Fix needed:** Either sync all SlickText contacts into our DB as blast-eligible, or send the blast directly via SlickText's broadcast API so it reaches all subscribers.

---

## 5. User received the blast twice

**What happened:** The blast was sent once from our system (confirmed via `messages` table — one spike). But the user is subscribed to both the `zarna` and `hello` textwords on SlickText. When our blast fires via SlickText's API using one textword, SlickText may have also triggered something on the other list.

**Fix needed:** Investigate whether SlickText sends duplicate delivery when the same number is on two textwords and the same message is sent. May need to consolidate to a single textword or suppress duplicates.

---

## 6. Bot told a user to "block the number" to unsubscribe

**What happened:** A subscriber asked how to stop getting texts. The bot said to "block the number" instead of "reply STOP". This is a compliance issue — SMS regulations require bots to direct users to reply STOP.

**Status: Fixed** (commit `f055b6a`) — added hard guardrail to always say "reply STOP".

---

## 7. Reply count attribution used wrong timestamp

**What happened:** The blast performance table counted replies starting from `sent_at`, which is recorded after the entire send loop finishes (~3 min after the first message goes out). Replies that arrived while the blast was still sending would have been missed.

**Status: Fixed** (commit `71f431b`) — added `started_at` column to `blast_drafts`, recorded before the send loop. Reply query now uses `COALESCE(started_at, sent_at)`. Note: for this specific blast the timing didn't matter (0 replies came in during the send window), but future blasts will be accurate.

---

## 8. Admin dashboard auto-refresh defaults to off

**What happened:** The "Auto-refresh" toggle on the admin dashboard defaulted to off, making it appear the page was stale/not updating.

**Fix needed:** Default auto-refresh to on, or at minimum persist the user's last setting.

---

## 9. Blast CTR used inflated `tracked_links.sent_to`

**What happened:** Insights Blast Performance computed CTR as `clicks / link_sent_to`. For the Malala blast, `sent_count` was 3,651 but `sent_to` on the link row was 5,172, so CTR looked lower than reality.

**Status: Fixed** — Insights now uses each blast’s `sent_count` as the CTR denominator (with `link_sent_to` only as fallback when `sent_count` is zero).

**Note:** The Conversions tab still shows CTR as `total_clicks / sent_to` per link (lifetime). If `sent_to` drifts, that number can still be off until we reconcile `sent_to` with actual sends.
