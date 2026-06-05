# Lovable prompts — "My Bot" page redesign

The backend `/api/bot-data` persists these fields (see
`operator/tests/test_bot_data_my_bot.py`) **and the runtime now sends them**:
when a brand-new fan first texts a creator, the bot sends the vCard MMS (if
`send_contact_card` is on) + the `first_message` (with compliance footer) and
then the normal AI reply. So these fields are now fully live — turning on
`send_contact_card` or filling `first_message` immediately changes what new fans
receive.

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
> Description) and **Links**. Remove the **Voice Style**, **Banned Words**, and
> any **Tone**, **Podcast URL**, **Media URLs**, and **Name variants**
> fields/cards — they aren't surfaced to creators. (Removing Banned Words is
> UI-only; the backend still honors `banned_words` if set, and the save payload
> must NOT include it.)
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
> - **Profile photo** bound to `profile_photo_url`. Show a circular/avatar
>   preview. **A square image is strongly recommended (~640×640)** — the backend
>   no longer rejects non-square images, but it crops to a circle for the contact
>   card, so a non-square photo may get cut off oddly. Show this as a helper note:
>   "Use a square photo (e.g. 640×640). Non-square photos still work but may crop
>   awkwardly in the round contact-card frame." Allowed: JPG/PNG, **max 5 MB**.
>   After the user picks a file, upload it via
>   `POST {API_BASE}/operator/blast/upload-image` (multipart field `image`,
>   `credentials:"include"`), then store the returned `url` in
>   `profile_photo_url`. If the upload returns a 400 with an `error` string
>   (e.g. too large), show that message to the user. Display the cropped circular
>   preview so the user can confirm it fits within the avatar frame.
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

## Prompt 4 — Reorder + visually polish the whole My Bot page

> Redesign the "My Bot" page for a clean, consistent, modern look. Keep all
> existing fields and their `/api/bot-data` bindings — this is layout + styling
> only, no API changes.
>
> **Reorder the cards top-to-bottom in this logical flow:**
> 1. **About** (Name, Description) — who the bot is
> 2. **SMS Profile** (SMS name, photo, "send contact card" toggle) — how you
>    appear to fans
> 3. **First Message** (welcome text + greyed compliance footer) — the first
>    thing a fan receives
> 4. **Links** (tickets, merch, book, youtube, website + custom links)
>
> **Visual system (apply consistently to every card):**
> - Uniform card style: same padding, border-radius, subtle border/shadow, and a
>   consistent card header = bold title + one-line muted subtitle. Even vertical
>   spacing between cards. Constrain content to a comfortable max-width (e.g.
>   ~720px) centered, instead of full-bleed.
> - Keep the "Edits this month X/20" meter in the page header, right-aligned;
>   don't repeat it per card.
> - Use a responsive 2-column grid for short single-line inputs (e.g. the Links
>   fields, and SMS name) on desktop, collapsing to 1 column on mobile.
>   Full-width for textareas (Description, First Message).
> - Consistent labels (small, medium-weight, muted) with helper text under each
>   field. Consistent input styling and focus states.
>
> **SMS Profile card specifics:**
> - Lay it out as a row: a circular **avatar** (the `profile_photo_url`, with an
>   "upload / change photo" affordance on hover) on the left, and the **SMS
>   name** field on the right. Below that, the "Send my contact card when a fan
>   first texts me" toggle as a clean switch row (label + helper on the left,
>   switch on the right).
>
> **First Message card specifics:**
> - Left/top: the editable `first_message` textarea.
> - Right/bottom: a small **live SMS preview** — render a chat bubble (incoming
>   style, like the fan's phone) showing the typed `first_message`, a blank
>   line, then the `compliance_footer` in muted/greyed text. Update live as they
>   type. Make clear the greyed part is auto-added and not editable.
>
> Use the existing design tokens / shadcn components and the site's accent color.
> Don't introduce a new color palette — match the rest of the dashboard.

## Verify after submitting

1. My Bot shows only About, SMS Profile, First Message, and Links (with Website
   + custom links) — no Voice Style / Banned Words / tone / podcast / media /
   name-variants.
2. Uploading a square photo previews in the avatar frame and persists after
   reload (`profile_photo_url` saved).
3. The contact-card toggle and SMS name save and reload correctly.
4. First Message saves; the greyed compliance line is visible, non-editable, and
   never sent in the POST body.
