# Lovable prompts — client dashboard template polish

These prompts make the client dashboard the standard template we hand each new
client. The backend changes are already built + tested (see
`operator/tests/test_inbox_full_numbers.py`).

Submit to Lovable **one at a time, in order**, and verify after each.

Backend API contract you can rely on (all already live):

- `GET {API_BASE}/api/inbox` → `conversations[]` now each include
  **`phone_number`** (full E.164, e.g. `+12125551234`) alongside the existing
  `phone_last4`.
- `GET {API_BASE}/api/inbox/<id>/thread` → `<id>` may be the **full phone
  number** OR the last-4 (both work). Response now includes top-level
  **`phone_number`** plus the full `messages[]` history (chronological, every
  message ever stored — no limit) with each message's `role`
  (`"user"` = the fan, `"assistant"` = the bot).
- `POST {API_BASE}/api/inbox/<id>/send` → `<id>` may be the full number or last-4.
- `GET {API_BASE}/api/audience/frequency` → `recent_blasted[]` now include
  `phone_number` too.
- `GET {API_BASE}/api/fan-of-the-week`, `/candidates`, and `/history` now all
  include **`phone_number`** (full). `POST /api/fan-of-the-week/select` now
  accepts `{ phone_number }` (full) as well as the legacy `{ phone_last4 }`.

---

## Prompt 1 — Show full phone numbers in the Inbox (no more asterisks)

> In the Inbox, stop masking fan phone numbers. The API now returns a full
> `phone_number` (E.164, e.g. `+12125551234`) on each `/api/inbox`
> conversation and on the `/api/inbox/<id>/thread` response.
>
> - In the conversation **list** (left column), replace the `****1234`
>   label with the full number formatted US-style, e.g. `+1 (212) 555-1234`.
>   Add a small `formatPhone(e164: string)` helper and reuse it everywhere.
> - In the conversation **detail header** (the panel that currently shows
>   "Fan #undefined"), show the fan's name if `fan.fan_name` exists, otherwise
>   the formatted full phone number — never "Fan #undefined".
> - Route the open-thread request by the full `phone_number` (the endpoint
>   accepts it); keep using `phone_last4` only as a fallback if `phone_number`
>   is missing.
> - Do the same in the Audience "recently blasted" list (use `phone_number`).
>
> Do not change `API_BASE` or any endpoint paths.

## Prompt 2 — Bot on the right, fan on the left, full history

> In the Inbox conversation thread, render messages "as if you are the bot":
>
> - Messages with `role === "assistant"` (the bot) align to the **right** with
>   the accent/peach bubble style.
> - Messages with `role === "user"` (the fan) align to the **left** with the
>   neutral/gray bubble style.
> - Render the **entire** `messages[]` array returned by the thread endpoint in
>   chronological order — do not cap, slice, or paginate it (the backend
>   already returns the full stored conversation).
>
> Keep timestamps and existing bubble styling; only the left/right side and
> color-by-role need to change.

## Prompt 3 — Show the full number on the Fan of the Week card

> On the Dashboard "Fan of the Week" card, stop showing the masked
> `Fan #3374`. The `/api/fan-of-the-week` response now includes a full
> `phone_number`. Show the fan's `fan_name` if present, otherwise the full
> phone formatted US-style (reuse the same `formatPhone` helper as the Inbox).
> Apply the same in the Fan-of-the-Week candidate picker and history list.
> When selecting a fan, POST `{ phone_number }` (full) instead of `phone_last4`.

## Prompt 4 — Move "Invite Member" under the Team table

> On the Team page, remove the "+ Invite Member" button from the top
> navigation / page header. Place it **below the team members table** instead,
> as a primary button (e.g. left-aligned under the list, label
> "+ Invite Member") that opens the same invite flow it currently triggers.
> Do not change the invite logic — only its location.

---

## Verify after submitting

1. Inbox list and the open conversation show full numbers like
   `+1 (212) 555-1234`, never `****1234` or "Fan #undefined".
2. In an open thread, the bot's replies sit on the right, the fan's messages on
   the left, and the whole history is visible.
3. The Team page shows "Invite Member" under the members table (not in the top
   nav), and it still opens the invite flow.
4. Live Shows tab is present in the dashboard nav (already shipped — just
   confirm it's there for the template).
