# Product Review Improvements — Implementation Plan

_Drafted from product review session with Didi, June 2026._

Six improvement areas were identified. Items 1–4 below are fully planned and ready to build. Item 5 (shorter messages) requires a separate design conversation and is intentionally last.

## Implementation status (June 2026)

| Item | Status | Notes |
|---|---|---|
| 1 — Show calendar | ✅ Built + tested | `app/brain/show_calendar.py`, generator + handler wired, Bandsintown API + config fallback, 30 unit tests. |
| 3 — Custom links | ✅ Built + tested | Config + generator injection + operator API read/write/sanitize. Frontend (Lovable) still TODO. |
| 4 — Analytics report | ✅ Backend built + tested | `get_media_kit_stats()` + `GET /api/analytics/report`. Frontend (Lovable) still TODO. |
| 2 — Image validation | ✅ Built + tested | `operator/app/image_validation.py` applied to both upload endpoints (5 MB + square). |
| 2 — vCard + first message (live send) | ✅ Built + tested | `app/messaging/contact_card.py` + `/vcard/performer/<slug>.vcf` route + `_process_twilio_message` wiring. Flow = vCard MMS → first_message → AI reply (option 3). OFF by default; gated by `send_contact_card` / `first_message`. Zarna's config untouched. |
| 5 — Photo blast UI | ⏸ Deferred | — |
| 6 — Shorter messages | ⏸ Deferred | — |

Backend test commands: `pytest tests/test_show_calendar.py tests/test_custom_links.py tests/test_image_validation.py` (main) and, with the operator test DB up, `cd operator && pytest tests/test_bot_data_my_bot.py tests/test_analytics_report.py`.

---

## Item 1 — Show-specific ticket links with date context

### Problem
When a fan texts "when are you coming to Chicago?", the bot sends a generic ticket page URL (`https://zarnagarg.com/tickets/`) with no show-specific date or venue. Didi's requirement: the reply should say something like _"We'd love to see you at [show] on [date]!"_ with a direct link to that show's tickets.

### Current state
- `creator_config/zarna.json` has a single `links.tickets` URL — no per-show data
- `app/brain/intent.py` handles `Intent.SHOW` with city/tour keywords
- `app/brain/generator.py` injects the generic ticket URL into the SHOW prompt

### Architecture — scrape + in-process cache (mirrors WSCC pattern)
New file `app/brain/show_calendar.py` — same pattern as `app/smb/knowledge.py`:
- On first SHOW intent per slug, fetch the creator's ticket page
- Parse show listings (city, venue, date, individual ticket URL)
- Cache result in-process per slug with **4-hour TTL** (auto-refreshes without a cron)
- Falls back to `links.tickets` if scrape fails or returns no shows

`creator_config/zarna.json` changes:
```json
"show_calendar_url": "https://zarnagarg.com/tickets/",
"show_calendar_tz": "America/New_York"
```

`app/brain/generator.py` changes (SHOW intent block):
- Call `show_calendar.get_shows(slug)` 
- Fuzzy-match the fan's message against show cities
- Inject matched show into prompt: _"Tell the fan you'd love to see them at [venue] in [city] on [date]. Use [ticket_url] as the link. Lead with that warmth before the URL."_
- If no city match: include all upcoming shows as a list, fall back to generic URL

### Ticketing platform — confirmed: Bandsintown

`zarnagarg.com/tickets/` embeds the **Bandsintown WordPress plugin**, which auto-syncs with her Bandsintown artist profile. No scraping needed — Bandsintown has a free public REST API:

```
GET https://rest.bandsintown.com/artists/zarna%20garg/events?app_id=zarna-bot
```

Returns JSON array with per-show objects:
- `datetime` — ISO 8601 timestamp
- `venue.name`, `venue.city`, `venue.region`, `venue.country`
- `offers[0].url` — direct ticket link for that show
- `title` — show name

No API key or auth required. Auto-updates whenever her team adds shows to Bandsintown.

**Interim config (manual fallback if API is unreachable):** Add `upcoming_shows` array to `creator_config/zarna.json`. The brain reads this as a fallback when the API call fails. Pre-populated with confirmed 2026 "Million Dollar Excuses" tour dates:

```json
"bandsintown_artist": "zarna garg",
"upcoming_shows": [
  { "city": "Lexington", "state": "KY", "venue": "Comedy Off Broadway", "date": "Jun 5-6, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Ridgefield", "state": "CT", "venue": "The Ridgefield Playhouse", "date": "Jun 9, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Great Barrington", "state": "MA", "venue": "Mahaiwe Performing Arts Center", "date": "Jun 19, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Newark", "state": "NJ", "venue": "New Jersey Performing Arts Center", "date": "Jun 20, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Austin", "state": "TX", "venue": "Cap City Comedy Club", "date": "Jun 25-27, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Portland", "state": "OR", "venue": "Helium Comedy Club", "date": "Jul 2-4, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Timonium", "state": "MD", "venue": "Magooby's Joke House", "date": "Jul 9-11, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Berlin", "state": "", "venue": "PUNCH L!NE Club Berlin", "date": "Aug 21, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Atlantic City", "state": "NJ", "venue": "Hard Rock Hotel & Casino", "date": "Sep 4, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Virginia Beach", "state": "VA", "venue": "Funny Bone Comedy Club", "date": "Sep 25-26, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Milwaukee", "state": "WI", "venue": "Turner Hall Ballroom", "date": "Oct 4, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "San Francisco", "state": "CA", "venue": "Palace of Fine Arts", "date": "Oct 10, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Cincinnati", "state": "OH", "venue": "Hard Rock Casino Cincinnati", "date": "Oct 23, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Cleveland", "state": "OH", "venue": "Mimi Ohio Theatre", "date": "Oct 24, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Port Chester", "state": "NY", "venue": "The Capitol Theatre", "date": "Nov 6, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Huntington", "state": "NY", "venue": "The Paramount", "date": "Nov 7, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Portland", "state": "ME", "venue": "State Theatre", "date": "Dec 4, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Atlanta", "state": "GA", "venue": "Buckhead Theatre", "date": "Dec 18, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "Washington", "state": "DC", "venue": "Lincoln Theatre", "date": "Dec 19, 2026", "ticket_url": "https://zarnagarg.com/tickets/" },
  { "city": "New York", "state": "NY", "venue": "Beacon Theatre", "date": "Dec 31, 2026", "ticket_url": "https://zarnagarg.com/tickets/" }
]
```

_Note: individual ticket URLs per show will be populated once the Bandsintown API integration is live, which returns per-show ticket links automatically._

### City-matching logic in `show_calendar.py`

| Situation | Injected prompt instruction |
|---|---|
| **Upcoming show in fan's city** | "Tell the fan you'd love to see them at [venue] in [city] on [date]. Use [ticket_url]. Lead with warmth before the URL." |
| **Recent past show (≤60 days ago) in fan's city** | "Zarna was just in [city] at [venue] on [date]. Acknowledge that warmly — 'We were just there!' — and invite them to check zarnagarg.com/tickets for when she's back." |
| **No match (future or recent past)** | List the next 3 upcoming shows with dates + cities, plus the general ticket URL. |

**Bandsintown API calls:**
- Upcoming: `GET /artists/zarna%20garg/events?app_id=zarna-bot` — returns future events only (automatic)
- Recent past: `GET /artists/zarna%20garg/events?app_id=zarna-bot&date=past&per_page=10` — check last 10 past shows for a city match within 60 days

Both responses are cached per-slug with a 4-hour TTL.

### Prompt language (hard requirement from Didi)
The SHOW prompt block must include: _"Always open with 'I would love to see you there!' or 'We'd love to see you at [show]!' before giving the link. Never just drop a bare URL. If the show has passed, be warm about it — never just say 'no upcoming shows.'"_

---

## Item 2 — vCard MMS + first message (Phase 2 wiring)

### Problem
The "SMS Profile" and "First Message" fields exist in the bot-data API and the Lovable UI is built. But when a new fan texts in, nothing sends the vCard or welcome message — the performer path has no first-text detection.

### Current state
- `operator/app/routes/api.py`: `sms_display_name`, `profile_photo_url`, `send_contact_card`, `first_message` fields are saved and returned
- `app/storage/postgres.py`: `is_first_message()` function exists but is **not called** anywhere in the live handler
- `app/smb/onboarding.py`: `_send_vcard_mms()` already works — sends a `.vcf` contact card via Twilio MMS
- Lovable "My Bot" page: SMS Profile card (Prompt 2) and First Message card (Prompt 3) from `docs/engineering/lovable_prompts_mybot.md` — submit these to Lovable if not already done

### What to build

**New file: `app/messaging/contact_card.py`** (~60 lines)
- Extract vCard-building + MMS-send logic from `app/smb/onboarding.py` into a shared helper
- `send_contact_card_mms(phone, display_name, photo_url, twilio_client)` — builds `.vcf`, sends as MMS

**`main.py` / `app/brain/handler.py`** changes:
1. After saving a new contact, call `is_first_message(phone, slug)`
2. If `True` and `send_contact_card` is enabled: call `send_contact_card_mms()`
3. Then send `first_message` text (with compliance footer appended) as a second message
4. Proceed to AI reply as normal — **or** suppress the AI reply on first text (decision needed: 2-message intro only, or 3 messages including AI reply?)

### Image spec enforcement (applies to all uploads)
Add validation to `operator/app/routes/blast.py` → `upload_image()`:
- **Max 5MB** — reject with: _"Image exceeds 5MB. Please compress or resize before uploading."_
- **Square only** — use Pillow to read dimensions; reject non-square with: _"Image must be square (e.g. 640×640). Yours was [W]×[H]."_
- **JPG or PNG only** for all uploads (remove gif/webp/pdf acceptance) — or keep those for blast-only and enforce square+5MB for all
- Return error messages the frontend can surface directly to the user

### Open decision
> When a brand-new fan texts in, should they receive:
> - (A) vCard MMS + first_message text + AI reply (3 messages)
> - (B) vCard MMS + first_message text only, no AI on first contact (2 messages)
>
> Option B feels cleaner — the welcome lands first, the AI conversation starts on the fan's second text. But worth confirming with Didi.

---

## Item 3 — Dynamic custom links in the UI

### Problem
The Links card in "My Bot" only supports fixed named links (tickets, merch, book, youtube, website). Creators may have other links they want the AI to reference (a course, a podcast, a Patreon, etc.) with no way to add them.

### What to build

**Backend — `operator/app/routes/api.py`:**
- Add `custom_links` array to the bot-data schema: `[{ "label": "...", "url": "...", "when_to_send": "..." }]`
- Accept in `POST /api/bot-data`; store in `creator_configs` JSON column
- Return in `GET /api/bot-data`

**Brain — `app/brain/generator.py`:**
- When `custom_links` is non-empty, append to system prompt:
  ```
  Additional links (use when relevant based on the description):
  - [Course on Indian cooking]: https://... — use when fan asks about cooking classes
  - [Patreon]: https://... — use when fan asks about exclusive content
  ```
- `when_to_send` field provides the AI explicit guidance on when to include each link
- If `when_to_send` is blank, the AI uses the label to infer context

**Lovable frontend — Links card:**
- "Add custom link" button creates a new row with: **Label** field + **URL** field + optional **"When should the AI send this?"** hint field
- Each row has an X to delete
- Save via existing `POST /api/bot-data`
- Helper text under the label field: _"Be specific — this tells the AI when to share this link. e.g. 'My online cooking course' not 'My stuff'."_

### AI cost
Zero cost at add/remove time. Each active custom link adds ~15 tokens to every conversation message. At Zarna's scale (~9,000 fan messages/month), 1 custom link ≈ $0.13/month — negligible.

---

## Item 4 — Analytics page (replacing Audience tab)

### Problem
The "Audience" page in the Lovable frontend shows basic subscriber counts. Didi wants a proper analytics view — something a creator could print and walk into a talent agency with.

### What to build

**New operator endpoint `GET /api/analytics/report`:**
Calls a new `get_media_kit_stats(slug)` function in `operator/app/queries.py` that adds:
- Total conversations (`COUNT` of messages where `conversation_turn = 1`)
- Longest conversation (`MAX(conversation_turn)` per phone)
- Fan tier breakdown (`GROUP BY fan_tier` on contacts — superfan / engaged / lurker / dormant)
- Top intents (`GROUP BY intent` on messages, top 6)
- Average messages per fan
- Engagement rate (% of bot messages where `did_user_reply = TRUE`)

Everything else (`total_subscribers`, `messages_by_day`, `tag_breakdown`, etc.) already exists in `get_overview_stats()`.

**Lovable frontend — rename "Audience" → "Analytics", redesign as 2-section layout:**

**Section 1 — "At a Glance"**
- 3 hero stat cards: **Total Subscribers · Total Conversations · Superfans**
- Growth callout: "+X new fans this week" with trend vs. prior week
- Bar chart: Messages per day (last 30 days)
- 4 smaller stat cards: Messages this week · Avg messages per fan · Longest conversation · Most active hour

**Section 2 — "Your Audience"**
- Pie chart: Fan tier breakdown (Superfan / Engaged / Lurker / Dormant)
- Bar chart: Top 6 fan intents (what fans text about most)
- Geographic: Top 5 states (upgraded from area codes)
- Tag table: Top fan tags with counts

**"Download Report" button** — triggers `window.print()` with print-specific CSS:
- Hides nav, sidebar, buttons
- Shows creator name + date at top
- Both sections format cleanly as a 1–2 page printout
- Looks like a proper media kit stat sheet

---

## Item 5 — Photo blast UI clarity

_Deferred. Revisit after items 1–4 are shipped._

Quick note for when we return: the blast pipeline is fully built in the operator service (`operator/app/routes/blast.py`, `operator/app/blast_sender.py`). The UI clarity issues are frontend-only — no backend changes needed. Focus areas: MMS vs SMS label clarity, image preview ("what the fan will receive"), and the upload flow.

---

## Item 6 — Shorter messages

_Deferred. Requires a full design conversation about intent-specific length rules._

Quick note: `style_rules` already says "max 3 sentences" but the LLM frequently ignores this for structured intents (SHOW, BOOK, PODCAST) because those prompts encourage enthusiasm + a link. The fix needs intent-type-specific length caps, not a global rule.

---

## Build order (recommended)

| # | Item | Effort | Dependencies |
|---|---|---|---|
| 1 | vCard / first message runtime | ~1 day | None — all building blocks exist |
| 2 | Analytics page | ~1–2 days | New queries (easy) + Lovable redesign |
| 3 | Custom links | ~1 day | Small schema change + brain injection + Lovable |
| 4 | Show calendar | ~1–2 days | **Blocked on confirming ticketing platform** |
| 5 | Photo blast UI | TBD | Deferred |
| 6 | Shorter messages | TBD | Deferred — needs design conversation |
