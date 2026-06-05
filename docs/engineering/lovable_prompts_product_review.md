# Lovable prompts — Custom Links + Analytics report

Frontend prompts for two backend features that are already live and tested:

1. **Custom Links** — creator-defined links the AI surfaces when relevant
   (Item 3). Backend: `operator/app/routes/api.py` + `operator/tests/test_bot_data_my_bot.py`.
2. **Analytics / Media Kit report** — a printable one-pager of headline stats
   (Item 4). Backend: `GET /api/analytics/report` + `operator/tests/test_analytics_report.py`.

Submit to Lovable **one at a time, in order**, verify after each. `{API_BASE}` is
the operator API base URL; all calls use `credentials: "include"`.

---

## Backend contracts (already live)

### Custom links — on `/api/bot-data`
`GET {API_BASE}/api/bot-data` (performer) returns, in addition to existing fields:
- `custom_links`: array of `{ "label": string, "url": string, "when_to_send": string }`.

`POST {API_BASE}/api/bot-data` accepts `custom_links` (full array — send the whole
list each save, it replaces the stored value). Server sanitizes on save:
- drops any row without a `label` AND an `http(s)` `url`,
- caps at **10** links, `label` ≤ 60 chars, `when_to_send` ≤ 160 chars.
So after saving, re-read `custom_links` from the response/refetch — the server's
cleaned version is the source of truth.

### Analytics report — `GET {API_BASE}/api/analytics/report`
Returns (all tenant-scoped, PII-free):
- Hero: `total_subscribers`, `total_conversations`, `superfans` (ints).
- Secondary: `total_fan_messages`, `messages_week`, `new_subs_week`,
  `avg_messages_per_fan` (float), `longest_conversation` (int = max turns in one
  conversation), `engagement_rate` (int 0–100 or `null`), `most_active_hour`
  (int 0–23 or `null`).
- `messages_by_day`: `[{ "date": "YYYY-MM-DD", "count": int }]` (last 30 days).
- `tier_breakdown`: `[{ "tier": "superfan|engaged|lurker|dormant", "count": int }]`.
- `top_intents`: `[{ "intent": string, "count": int }]` (already sorted desc).
- `top_area_codes`: `[{ "area_code": string, "count": int }]`.

---

## Prompt 1 — Add the "Custom Links" card to the My Bot page

> On the **My Bot** page, add a new card titled **"Custom Links"** with subtext
> "Extra links your bot can share with fans when it's relevant."
>
> Bind it to the `custom_links` array from `GET /api/bot-data` (each item is
> `{ label, url, when_to_send }`). Render an editable list where each row has:
> - **Label** (text) — helper: "What this link is, e.g. 'Cooking Course'."
> - **URL** (text) — helper: "Must start with http:// or https://."
> - **When to send** (text) — helper: "Describe when the bot should share it,
>   e.g. 'when a fan asks about cooking classes'. The AI uses this to decide."
>
> Add an **"+ Add link"** button (disabled once there are 10 rows — show a small
> "Max 10 links" note) and a remove (trash) button per row. Validate inline that
> URL starts with `http`. On **Save**, `POST /api/bot-data` with the full
> `custom_links` array, then refetch and re-render from the response (the server
> strips invalid rows and trims long fields — show the cleaned result, don't
> assume the local state was kept verbatim).
>
> Place this card after **Links** and before **Banned Words**. Match the existing
> card styling.

## Prompt 2 — Build the "Analytics" report page

> Create/replace the **Analytics** page (rename the existing "Audience" page to
> **Analytics** if present). Fetch `GET {API_BASE}/api/analytics/report` once on
> load (`credentials:"include"`). Design it as a clean, screenshot-worthy
> **media-kit one-pager** a creator could hand to a talent agency.
>
> **Header:** the creator's name + "Audience Report" + today's date, and a
> **"Download PDF"** button (top-right) that calls `window.print()`.
>
> **Section A — "At a glance":** three large hero stat cards in a row:
> - **Subscribers** = `total_subscribers`
> - **Conversations** = `total_conversations`
> - **Superfans** = `superfans`
> Each big number with a small muted label.
>
> **Section B — secondary stats** as a compact grid of smaller stat tiles:
> `messages_week` ("Messages this week"), `new_subs_week` ("New subs this week"),
> `avg_messages_per_fan` ("Avg messages / fan"), `longest_conversation`
> ("Longest conversation, turns"), `engagement_rate` ("Reply rate", render as
> `{n}%`, or "—" when `null`), `most_active_hour` (format as a 12-hour time like
> "8 PM", or "—" when `null`).
>
> **Section C — charts** (use the charting lib already in the project, e.g.
> recharts):
> - **Messages over time**: a line/area chart from `messages_by_day`
>   (x = date, y = count).
> - **Audience breakdown**: a **pie/donut chart** from `tier_breakdown`
>   (slice per tier, with a legend + counts). Title it "Fan tiers".
> - **What fans talk about**: a horizontal bar chart from `top_intents`
>   (intent label vs count). Title it "Top topics".
> - **Top area codes**: a small bar list or table from `top_area_codes`.
>
> Handle empty/`null` gracefully (show "—" or "Not enough data yet" for empty
> arrays). Use the existing design tokens / shadcn components and the site accent
> color — no new palette.

## Prompt 3 — Print/PDF styling for the report

> Add print-specific CSS to the Analytics page so **"Download PDF"**
> (`window.print()`) produces a clean one-pager:
> - Hide the app nav/sidebar, buttons, and any interactive chrome in `@media
>   print` (`.no-print { display: none }`); keep only the report content.
> - Force a light background, dark text, and ensure charts render (set explicit
>   chart width/height; avoid relying on hover/animation).
> - Add the creator name + report title + date as a print header, and a small
>   "Generated by [product] · {date}" footer.
> - Use `break-inside: avoid` on each card/chart so sections don't split across
>   pages; target a single A4/Letter page if it fits, otherwise two.

---

## Verify after submitting

1. **Custom Links:** add a row (label + https URL + when-to-send), save, reload —
   it persists. Add a row with a non-`http` URL or blank label, save — it's
   dropped on refetch. Adding an 11th link is blocked.
2. **Analytics:** the three hero numbers, the secondary tiles, and all charts
   render from real data; empty states show "—" rather than crashing.
3. **PDF:** "Download PDF" opens the print dialog and the preview shows only the
   report (no sidebar/buttons), on a light background, without cut-off charts.
