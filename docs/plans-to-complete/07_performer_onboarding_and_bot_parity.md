# Plan: Performer Onboarding & Bot Feature Parity
_Created: May 2, 2026_
_Updated: Jul 3, 2026 — added Part 8 (compliance disclosure on live-show keyword joins)_
_Status: Plan finalized. Ready for implementation._

---

## What This Is

Businesses currently have a full set of bot features — custom welcome messages, a signup question, a contact card (name + photo sent as a vCard MMS on first text), and an outreach invite message. Performers have none of these. This plan brings performers to full feature parity with businesses on all bot settings, adds a proper performer subscriber table, and fixes a pre-existing bug where the business `send_contact_card` toggle in My Bot doesn't actually take effect at runtime.

---

## Background & Context

### What businesses currently have in My Bot
- `welcome_message` — custom text sent to new subscribers on opt-in
- `signup_question` — open-ended onboarding question appended to the welcome
- `send_contact_card` — toggle that sends a vCard MMS (contact name + photo) so subscribers can save the business to their contacts
- `logo_url` — the photo embedded in the contact card
- `outreach_invite_message` — default copy for blast invite SMS
- `tone`, `address`, `hours`, `website`, `tracked_links`, `display_name`

### What performers currently have in My Bot
- `name`, `bio`, `description`, `voice_style`, `tone`
- `website_url`, `podcast_url`, `media_urls`, structured `links` (tickets, merch, book, youtube)
- `banned_words`, `name_variants`
- **No welcome message** — first-message handling is fully AI-generated
- **No contact card**
- **No signup question**
- **No outreach invite message**
- **No subscriber table** — fans are tracked only as contacts in the `messages` table; there is no explicit opt-in record

### Pre-existing bug
The business `send_contact_card` toggle saves to `smb_bot_config` in the DB via My Bot, but `app/smb/onboarding.py` reads the flag from `tenant.raw` (the on-disk JSON config file) which never sees DB overrides. So flipping the toggle in My Bot has no effect until the file is manually updated. This plan fixes it.

---

## Key Design Decisions

### 1. Live show keyword + subscribe interaction
When a fan's very first text matches a live show keyword:
- The fan is recorded as a subscriber (new `performer_subscribers` row)
- The contact card MMS fires if enabled — it's useful context regardless of why they first texted
- The welcome text does **NOT** fire — the live show join confirmation already acts as the welcome, and sending both would be jarring
- **The A2P/CTIA compliance disclosure DOES fire** — because the AI/welcome path is skipped for keyword-only joins (`suppress_ai=True`), the join confirmation is the fan's opt-in confirmation and must therefore carry the disclosure itself. This is handled in Part 8.
- This is achieved by splitting onboarding into two responsibilities (see Part 4 below)

### 2. No re-subscription for returning fans
A fan who texted months ago and returns is not treated as a new subscriber. The `is_first_message` check uses the existing `messages` table — if any prior message exists for that `phone_number`, onboarding is skipped entirely. The `performer_subscribers` `ON CONFLICT DO NOTHING` insert handles any edge case idempotently.

### 3. Welcome message vs AI greeting
- If a performer sets a `welcome_message` → that text is sent as the entire first reply (AI is bypassed for that message)
- If no `welcome_message` is set → the AI brain handles the greeting as it does today, but the compliance line is appended to the AI's output
- Either way, the compliance line is always present on the first message

### 4. A2P 10DLC compliance
Every fan's **first outbound message** — whatever form it takes — must carry the disclosure. There are three first-message shapes and each must include it:
- Static `welcome_message` (Part 5) → disclosure appended
- AI-generated greeting when no `welcome_message` is set (Part 5) → disclosure appended to AI output
- Live-show join confirmation for keyword-only joins (Part 8) → disclosure appended to the confirmation copy

The canonical disclosure text is the existing `COMPLIANCE_FOOTER` constant in `app/messaging/contact_card.py` (`"Msg & data rates may apply. Reply STOP to opt out, HELP for help."`). All three paths reuse that single constant — no redefinitions — so wording stays consistent and is editable via the `PERFORMER_COMPLIANCE_FOOTER` env var. This mirrors the business welcome flow which already includes CTIA disclosures.

### 5. Contact card defaults
- `send_contact_card` defaults to `false` for performers (businesses default to `true`)
- A graceful fallback: if `profile_photo_url` is missing or broken, a name-only vCard is sent (no photo) rather than erroring
- Broken URL validation on save is deferred — handled gracefully at vCard build time only

---

## Implementation Plan

### Part 1 — Database: `performer_subscribers` table

**File:** `app/storage/postgres.py`

Add a new `_PERFORMER_MIGRATIONS` tuple (modeled after `_SMB_MIGRATIONS`) and wire it into the startup migration block. Schema:

```sql
CREATE TABLE IF NOT EXISTS performer_subscribers (
    id            BIGSERIAL    PRIMARY KEY,
    phone_number  TEXT         NOT NULL,
    creator_slug  TEXT         NOT NULL,
    subscribed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    status        TEXT         NOT NULL DEFAULT 'active',
    UNIQUE (phone_number, creator_slug)
);
CREATE INDEX IF NOT EXISTS idx_performer_subscribers_slug
    ON performer_subscribers(creator_slug, status);
```

The `UNIQUE` constraint + `ON CONFLICT DO NOTHING` on insert handles idempotency and race conditions natively — no application-level locking needed.

---

### Part 2 — API: five new performer My Bot fields

**File:** `operator/app/routes/api.py`

**`GET /api/bot-data` performer response** adds:
- `welcome_message` (string, default `""`)
- `signup_question` (string, default `""`)
- `send_contact_card` (boolean, default `false`)
- `profile_photo_url` (string, default `""`)
- `outreach_invite_message` (string, default `""`)

**`POST /api/bot-data` performer allowlist** adds the same five field names to `allowed_performer`.

**File:** `docs/engineering/lovable_frontend_wiring.md`

Update the performer `GET /api/bot-data` response shape and the "Performer allowed fields" line in the POST section.

---

### Part 3 — Performer vCard route

**File:** `operator/app/routes/api.py`

New route: `GET /performer/vcard/<slug>.vcf`

- Reads `name` and `profile_photo_url` from `bot_configs.config_json` for that slug
- Extract the existing `_build_vcard` business logic into a shared private helper `_build_vcard_from_parts(display_name, photo_url)` that both the business route and the new performer route call
- Cache in `_vcard_cache` using a prefixed key `performer::<slug>` to avoid collisions with the existing business keys
- On `POST /api/bot-data` when `profile_photo_url` is in the update payload, drop `performer::<slug>` from `_vcard_cache` so the next request rebuilds with the new photo
- Graceful fallback: if `profile_photo_url` is empty or the image fetch fails, build a name-only vCard (no `PHOTO` line) rather than returning 404

---

### Part 4 — New performer onboarding module

**File:** `app/performer/onboarding.py` _(new file)_

Two responsibilities cleanly separated:

**Function A: `ensure_performer_subscriber(phone_number, creator_slug, channel) -> bool`**

Runs unconditionally for every new inbound message, even on keyword-only live show joins. Returns `is_new_fan` (True when this was the fan's first-ever message) so the webhook layer can decide whether to append the compliance disclosure to a live-show join confirmation (Part 8). Steps:
1. Check `is_first_message` against the `messages` table (uses existing `PostgresStorage.is_first_message`)
2. If not first → return `False` immediately (no-op)
3. If first → `INSERT INTO performer_subscribers ... ON CONFLICT DO NOTHING`
4. Load `send_contact_card` and `profile_photo_url` from `bot_configs.config_json`
5. If `send_contact_card=True` → fire vCard MMS in a daemon thread (same pattern as `app/smb/onboarding.py`'s `_send_vcard_mms`; use performer vCard URL `/performer/vcard/<slug>.vcf`)
6. Return `True`

**Note on ordering / `is_first_message`:** `ensure_performer_subscriber` reads `is_first_message` and must run **before** the brain saves any inbound row to `messages`, otherwise the fan would already look "returning." In `main.py` it runs right after the live-show signup block and before the `suppress_ai` early return (Part 6), which is before the brain executes — correct. The returned `is_new_fan` is captured and reused by Part 8 so the first-message check happens exactly once per inbound.

**Function B: `get_performer_welcome_reply(creator_slug, phone_number, storage) -> tuple[str | None, bool]`**

Returns `(reply_text_or_none, is_new_fan)`. Only runs when the brain is about to generate a reply. Steps:
1. Check `is_first_message`
2. If not first → return `(None, False)`
3. If first:
   - Load `welcome_message` and `signup_question` from `bot_configs.config_json`
   - Build reply:
     - Base = `welcome_message` if set, else `None`
     - If `signup_question` is set → append on new line after base
     - Always append: `"\n\nMsg & data rates may apply. Reply STOP to opt out."`
   - If `welcome_message` was set → return `(full_string, True)` — caller uses this directly, skips AI
   - If `welcome_message` was NOT set → return `(None, True)` — caller lets AI run but appends compliance to AI's output

---

### Part 5 — Integration into `ZarnaBrain.handle_incoming_message`

**File:** `app/brain/handler.py`

After `self.storage.save_contact(phone_number)` (step 1 of the existing handler), add:

1. Call `get_performer_welcome_reply(self.slug, phone_number, self.storage)` → `(welcome_reply, is_new_fan)`
2. If `welcome_reply` is set (custom welcome exists):
   - Save it to storage as the assistant's reply
   - Return it immediately — full brain pipeline skipped for this message
3. If `welcome_reply` is `None` and `is_new_fan=True`:
   - Brain pipeline runs normally
   - After `generate_zarna_reply` returns, append the compliance line to the AI's output before saving and returning it
4. If `is_new_fan=False` → nothing changes, normal pipeline as today

---

### Part 6 — Integration into `main.py`

**File:** `main.py`

Both the `slicktext_webhook` and Twilio webhook handlers get the same addition. After the live show signup block and **before** the `suppress_ai` early return:

```python
# After:
signup_res = _safe_try_live_show_signup(raw_phone, raw_body, channel)

# Add (new):
is_new_fan = False
if raw_phone:
    is_new_fan = ensure_performer_subscriber(raw_phone, brain.slug, channel)

# The join confirmation send (existing) now takes is_new_fan so Part 8 can
# append the compliance disclosure for first-time opt-ins:
if signup_res.join_confirmation_sms and signup_res.confirmation_phone:
    _send_join_confirmation_async(..., is_new_fan=is_new_fan)
elif signup_res.suppress_ai and is_new_fan:
    # Part 8: keyword join with no confirmation copy (e.g. "other" category) —
    # still send a minimal disclosure-bearing opt-in message.
    _send_join_confirmation_async(
        ..., body=MINIMAL_OPT_IN_CONFIRMATION, is_new_fan=True
    )

# Before:
if signup_res.suppress_ai:
    return ...  # early return unchanged
```

This achieves the correct behavior:
- Live show keyword-only join (comedy/live-stream) → subscriber created, contact card sent, join confirmation sent **with disclosure appended (if new fan)**, **no welcome text**
- Live show keyword-only join (`other` category, no confirmation copy) → subscriber created, contact card sent, **minimal disclosure-bearing opt-in SMS sent (if new fan)**
- First fan text with no live show → subscriber created, contact card sent, brain runs and returns welcome text (disclosure appended in Part 5)
- Returning fan → `ensure_performer_subscriber` returns `False`; no disclosure re-sent, everything proceeds normally

---

### Part 7 — Fix existing `send_contact_card` bug for businesses

**Three small changes:**

1. **`app/smb/tenants.py`** — Add `send_contact_card: bool = True` as a field on the `BusinessTenant` dataclass, and populate it from `cfg.get("send_contact_card", True)` in the factory function

2. **`app/smb/brain.py`** — In `_apply_bot_config_overrides`, add `"send_contact_card": "send_contact_card"` to `field_map` so DB overrides from `smb_bot_config` propagate to the tenant object at runtime

3. **`app/smb/onboarding.py`** — Change line 152 from `tenant.raw.get("send_contact_card", True)` to `tenant.send_contact_card`

---

### Part 8 — Compliance disclosure on live-show keyword joins

**Problem being fixed:** Keyword-only live show joins set `suppress_ai=True`, so the webhook returns *before* the brain runs. That means the Part 5 compliance-append logic (which lives in the brain path) never executes for these fans. Today their only message is the live-show join confirmation, which carries no A2P/CTIA disclosure — yet texting the keyword *is* their opt-in. Parts 1–7 add the subscriber row and contact card for these fans but do **not** by themselves deliver the disclosure. Part 8 closes that gap.

**Design principle:** For a keyword-only joiner, the join confirmation *is* the opt-in confirmation. The disclosure must ride on that message (or on a minimal standalone message when there is no confirmation copy). We do **not** resurrect the welcome/AI path for these fans — that stays suppressed per Key Design Decision #1 to avoid a jarring double-text.

**Files & changes:**

**1. `main.py` — `_send_join_confirmation_async`**

- Import the existing `COMPLIANCE_FOOTER` from `app/messaging/contact_card.py` (single source of truth — no new constant).
- Add an `is_new_fan: bool = False` parameter.
- When `is_new_fan` is True, append `"\n\n" + COMPLIANCE_FOOTER` to the outbound body before sending. When False (returning fan re-joining a later show), send the confirmation copy unchanged — no repeated disclosure nagging.
- Define a module-level `MINIMAL_OPT_IN_CONFIRMATION` used for the no-confirmation-copy case, e.g. `"You're on the list!"` (the footer is appended by the same code path, so the fan receives `"You're on the list!\n\nMsg & data rates may apply. Reply STOP to opt out, HELP for help."`).

**2. `main.py` — both webhook handlers (Twilio + SlickText)**

- Capture `is_new_fan` from `ensure_performer_subscriber` (Part 6).
- Pass `is_new_fan` into `_send_join_confirmation_async` for the normal confirmation case.
- Add the `elif signup_res.suppress_ai and is_new_fan:` branch that fires the minimal opt-in message when there is no `join_confirmation_sms` (covers `other`-category shows, which currently send nothing — see `app/live_shows/signup.py` line 131 where only `comedy`/`live_stream` produce copy).
- Ordering unchanged: this all happens before the `suppress_ai` early return.

**3. No change to `app/live_shows/signup.py` copy generation**

The confirmation copy pools in `app/live_shows/join_confirmations.py` stay disclosure-free — the footer is appended at send time in `main.py`. This keeps the personality copy clean and reusable, and keeps the disclosure wording in exactly one place. (If a future creator wants disclosure baked into custom copy, that's a separate decision.)

**Why gate on `is_new_fan` rather than always append?**
- Correctness: the legal requirement is the disclosure on the **opt-in / first contact**, which `is_new_fan` precisely identifies.
- UX: a fan who joins show A tonight and show B next week shouldn't get the same boilerplate every time.
- Safety fallback: if we ever can't determine newness (e.g. `ensure_performer_subscriber` errored and returned `False`), we simply skip the append — but the brain-path append (Part 5) still covers any non-suppressed message, so no first message ever ships without a disclosure. If Legal later prefers "disclosure on every confirmation," flipping this to always-append is a one-line change.

**Dependency note:** Part 8 relies on `is_new_fan` from Part 4/6. If Part 8 must ship *before* the rest of Plan 07, substitute a direct `storage.is_first_message(phone_number)` check in `main.py` (guarded in try/except, defaulting to not-appending on error) in place of the `ensure_performer_subscriber` return value. The rest of Part 8 is unchanged.

---

## Files Changed Summary

| File | Change |
|---|---|
| `app/storage/postgres.py` | Add `_PERFORMER_MIGRATIONS` tuple; wire into startup |
| `operator/app/routes/api.py` | 5 new fields in GET/POST bot-data; new performer vCard route; shared vCard helper; cache invalidation on photo URL save |
| `app/performer/onboarding.py` | **New file** — `ensure_performer_subscriber` + `get_performer_welcome_reply` |
| `app/brain/handler.py` | Call `get_performer_welcome_reply`; handle early return + compliance append |
| `main.py` | Call `ensure_performer_subscriber` (capture `is_new_fan`) in both Twilio and SlickText webhook handlers; append `COMPLIANCE_FOOTER` to join confirmations for new fans; send minimal disclosure-bearing opt-in when a keyword join has no confirmation copy (Part 8) |
| `app/smb/tenants.py` | Add `send_contact_card` field to `BusinessTenant` dataclass |
| `app/smb/brain.py` | Add `send_contact_card` to `_apply_bot_config_overrides` field_map |
| `app/smb/onboarding.py` | Read `tenant.send_contact_card` instead of `tenant.raw.get(...)` |
| `docs/engineering/lovable_frontend_wiring.md` | Update performer bot-data GET response shape and POST allowed fields |

---

## Testing Plan

### Unit tests

**`tests/test_performer_onboarding.py`** (new file)

- `test_ensure_subscriber_first_message` — mock `is_first_message=True`, confirm row inserted into `performer_subscribers`
- `test_ensure_subscriber_returning_fan` — mock `is_first_message=False`, confirm no DB insert
- `test_ensure_subscriber_idempotent` — call `ensure_performer_subscriber` twice for the same fan; confirm second call is a no-op (ON CONFLICT DO NOTHING)
- `test_contact_card_sent_when_enabled` — mock `send_contact_card=True` + `profile_photo_url` set, confirm `_send_vcard_mms` thread is started
- `test_contact_card_skipped_when_disabled` — mock `send_contact_card=False`, confirm no vCard send
- `test_contact_card_skipped_when_no_photo` — `send_contact_card=True` but `profile_photo_url=""`, confirm name-only vCard (or skip entirely per final implementation)
- `test_welcome_reply_with_custom_message` — `welcome_message` set, confirm returned string contains it + compliance line
- `test_welcome_reply_with_signup_question` — both `welcome_message` and `signup_question` set, confirm both in returned string + compliance line
- `test_welcome_reply_no_custom_message` — `welcome_message` not set, confirm `get_performer_welcome_reply` returns `(None, True)` not `(None, False)`
- `test_welcome_reply_returning_fan` — `is_first_message=False`, confirm `(None, False)` regardless of config

**`tests/test_live_show_compliance.py`** (new file — Part 8)

- `test_join_confirmation_appends_footer_for_new_fan` — comedy keyword join, `is_new_fan=True`; confirm sent body ends with `COMPLIANCE_FOOTER`
- `test_join_confirmation_no_footer_for_returning_fan` — comedy keyword join, `is_new_fan=False`; confirm sent body has no footer and equals the raw confirmation copy
- `test_other_category_new_fan_gets_minimal_optin` — `other`-category keyword join, `suppress_ai=True`, no confirmation copy, `is_new_fan=True`; confirm `MINIMAL_OPT_IN_CONFIRMATION` + footer is sent
- `test_other_category_returning_fan_gets_nothing` — same but `is_new_fan=False`; confirm no message is sent
- `test_footer_uses_single_constant` — assert the appended text is exactly `contact_card.COMPLIANCE_FOOTER` (guards against wording drift)
- `test_suppress_ai_still_returns_early` — confirm the webhook still short-circuits (no brain call) after sending the disclosure

**`tests/test_performer_vcard.py`** (new file)

- `test_vcard_route_returns_vcf` — GET `/performer/vcard/<slug>.vcf`, confirm `text/vcard` mimetype and `FN:` contains the performer name
- `test_vcard_route_unknown_slug` — confirm 404
- `test_vcard_cache_invalidated_on_photo_update` — POST `/api/bot-data` with `profile_photo_url`, confirm `performer::<slug>` dropped from `_vcard_cache`

**`tests/test_smb_onboarding.py`** (existing file — additions)

- `test_send_contact_card_db_override_respected` — set `send_contact_card=False` in `smb_bot_config`, confirm `_send_vcard_mms` is NOT called (tests the bug fix)
- `test_send_contact_card_default_true` — no override set, confirm default is still `True`

### Integration / manual tests

- **New performer, no welcome message configured** — text from a new number; confirm: (1) `performer_subscribers` row created, (2) AI greeting fires, (3) AI greeting has compliance line appended, (4) no contact card (default `send_contact_card=False`)
- **New performer, welcome message + signup question set** — confirm: (1) static welcome text sent (not AI), (2) signup question appended, (3) compliance line appended, (4) AI does NOT double-reply
- **New performer, contact card enabled + photo set** — confirm vCard MMS received on a real test device
- **Live show keyword, new fan (comedy/live-stream)** — confirm: (1) join confirmation sent **with `COMPLIANCE_FOOTER` appended**, (2) welcome text NOT sent, (3) contact card IS sent (if enabled), (4) `performer_subscribers` row created
- **Live show keyword, new fan (`other` category, no confirmation copy)** — confirm the minimal opt-in message (`MINIMAL_OPT_IN_CONFIRMATION` + footer) is received; `performer_subscribers` row created
- **Live show keyword, returning fan** — confirm the join confirmation is sent with **no** footer and no minimal opt-in message (no duplicate disclosure)
- **Returning fan texts** — confirm no duplicate `performer_subscribers` insert, no welcome message sent again, normal AI reply
- **Business `send_contact_card` toggle** — set to `false` via My Bot UI (POST `/api/bot-data`), trigger new subscriber flow, confirm vCard MMS is NOT sent (tests the bug fix end-to-end)

### Regression checks

- Existing Zarna fan conversations unaffected (returning fan path is a no-op)
- Business SMB onboarding still sends contact card by default (default remains `True`)
- Live show join confirmation still fires correctly; `suppress_ai` behavior unchanged
- `GET /api/bot-data` for businesses still returns all existing fields

---

## What Is Explicitly Out of Scope

- **Broken URL validation on `profile_photo_url` save** — deferred; graceful fallback at vCard build time is sufficient for now
- **Multi-performer webhook routing** — this plan works within the current single-brain-per-process architecture; full multi-performer webhook routing is a separate infrastructure project
- **Performer subscriber blast targeting** — `performer_subscribers` creates the data foundation, but UI for blasting only to "opted-in subscribers" vs "all contacts" is a separate feature
- **Re-subscription flow** — if a fan previously STOP'd and texts again, they are not automatically re-subscribed; that is an intentional limitation for now

---

_Next step: implement in the order listed (Parts 1 → 8). Each part is independently testable. Part 8 depends on Part 4/6 for `is_new_fan`; if it must ship first, use the standalone `is_first_message` fallback described in that part._
