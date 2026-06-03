# Lovable prompts — "My Bot" page redesign (Phase 1)

Phase 1 = UI + config storage only. The backend `/api/bot-data` already
persists the new fields (see `operator/tests/test_bot_data_my_bot.py`). The
runtime that actually *sends* the first message / contact card is Phase 2 and is
NOT wired yet — so these fields save and display, but don't change live texting
behavior until Phase 2 ships.

Submit to Lovable **one at a time, in order**, verify after each.

## Backend contract (already live)

`GET {API_BASE}/api/bot-data` (performer) returns, in addition to the existing
fields:
- `links`: now also includes a `website` key (move Website here).
- `sms_display_name` (string) — the name shown to fans / on the contact card.
- `profile_photo_url` (string URL) — the SMS profile / contact-card photo.
- `send_contact_card` (boolean) — whether to send the contact card on first text.
- `first_message` (string) — the opt-in / welcome message.
- `compliance_footer` (string, **read-only**) — auto-appended disclosure; show
  it greyed-out and non-editable.

`POST {API_BASE}/api/bot-data` accepts (merge-saves): `name`, `description`,
`links`, `banned_words`, `sms_display_name`, `profile_photo_url`,
`send_contact_card`, `first_message`. (`compliance_footer` is server-owned —
never send it.)

**Image upload:** `POST {API_BASE}/operator/blast/upload-image`, multipart form
field `image`, `credentials: "include"` → returns `{ "url": "https://…" }`.
Save that URL into `profile_photo_url` via `POST /api/bot-data`.

---

## Prompt 1 — Slim down the My Bot page

> On the "My Bot" page, keep only these existing cards: **About** (Name +
> Description), **Links**, and **Banned Words**. Remove the **Voice Style**
> card and any **Tone**, **Podcast URL**, **Media URLs**, and **Name variants**
> fields/cards — they aren't wired to anything.
>
> In the **Links** card, add a **Website** field bound to `links.website`
> (alongside tickets, merch, book, youtube). Keep saving via
> `POST /api/bot-data`. Don't change the page's data-loading otherwise.

## Prompt 2 — Add the "SMS Profile" card

> Add a new card titled **"SMS Profile"** with subtext "How you appear to fans
> in their texts."
>
> Fields:
> - **SMS name** (text) bound to `sms_display_name`. Helper: "The name fans see
>   — e.g. on your contact card."
> - **Profile photo** bound to `profile_photo_url`. Show a **square** image
>   picker/preview (a circular/avatar crop is fine). Constrain uploads to a
>   square image, recommend ~640×640, max ~1 MB, JPG/PNG. After the user picks
>   a file, upload it via `POST {API_BASE}/operator/blast/upload-image`
>   (multipart field `image`, `credentials:"include"`), then store the returned
>   `url` in `profile_photo_url`. Display the cropped preview so the user can
>   confirm it fits within the avatar frame.
>
> Then, **underneath**, add a clearly-worded toggle bound to
> `send_contact_card`:
> - Label: **"Send my contact card when a fan first texts me"**
> - Helper: "We'll text new fans a tap-to-save contact (your SMS name + photo)
>   so you show up as a real contact, not just a number."
>
> Save all three via `POST /api/bot-data`.

## Prompt 3 — Add the "First Message" card with auto compliance footer

> Add a new card titled **"First Message"** with subtext "The first text a new
> fan gets when they reach out."
>
> - A multiline textarea bound to `first_message`. Helper: "Optional. A quick
>   intro in your voice. Sent once, only to brand-new fans."
> - **Directly below the textarea**, render the `compliance_footer` value from
>   the API as **greyed-out, non-editable** text inside a subtle bordered box,
>   prefixed with a small label: "Always added automatically (required for
>   compliance):". This must NOT be editable and must NOT be sent back to the
>   API.
> - Show a tiny combined preview: the user's `first_message`, a blank line, then
>   the greyed `compliance_footer`, so they see exactly what fans receive.
>
> Save `first_message` via `POST /api/bot-data`.

---

## Verify after submitting

1. My Bot shows only About, Links (with Website), Banned Words, SMS Profile,
   First Message — no Voice Style / tone / podcast / media / name-variants.
2. Uploading a square photo previews in the avatar frame and persists after
   reload (`profile_photo_url` saved).
3. The contact-card toggle and SMS name save and reload correctly.
4. First Message saves; the greyed compliance line is visible, non-editable, and
   never sent in the POST body.
