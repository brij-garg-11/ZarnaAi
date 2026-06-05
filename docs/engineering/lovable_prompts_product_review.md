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
  `avg_messages_per_fan` (float), `longest_conversation` (int = most messages
  exchanged with a single fan), `engagement_rate` (int 0–100 or `null`),
  `most_active_hour` (int 0–23 or `null`).
- `messages_by_day`: `[{ "date": "YYYY-MM-DD", "count": int }]` (last 30 days).
- `tier_breakdown`: `[{ "tier": "superfan|engaged|lurker|dormant", "count": int }]`.
- `top_intents`: `[{ "intent": string, "count": int }]` (already sorted desc).
- `top_area_codes`: `[{ "area_code": string, "count": int }]`.

---

## Prompt 1 — Add custom links inside the existing "Links" card

> On the **My Bot** page, add custom links **inside the existing Links card** (do
> NOT create a separate card). Keep the predefined fields (Tickets, Merch, Book,
> YouTube, Website). Below them, add an **"+ Add link"** button.
>
> Each click adds one custom-link row bound to the `custom_links` array from
> `GET /api/bot-data` (each item is `{ label, url, when_to_send }`). Render each
> row as three compact inputs on one line plus a remove (trash) icon:
> - **Label** — e.g. "Cooking Course".
> - **URL** — must start with http:// or https:// (validate inline).
> - **When to send** — helper: "When the bot should share it, e.g. 'when a fan
>   asks about cooking classes'. The AI uses this to decide."
>
> Disable "+ Add link" once there are 10 custom rows (show a small "Max 10 links"
> note). On **Save**, `POST /api/bot-data` with the full `custom_links` array,
> then refetch and re-render from the response (the server strips invalid rows and
> trims long fields — show the cleaned result, don't assume local state was kept
> verbatim). Match the existing card styling.

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
> ("Longest conversation, messages"), `engagement_rate` ("Reply rate", render as
> `{n}%`, or "—" when `null`), `blast_reply_rate` ("Avg blast reply rate", render
> as `{n}%`, or "—" when `null`; add a tiny subline "avg across {blasts_counted}
> campaigns" when `blasts_counted > 0`. This is the **average of each campaign's
> reply rate** — every campaign weighted equally — and small test sends under 15
> recipients are excluded. Do NOT show a pooled "X of Y replied" ratio next to it,
> since this is a per-campaign average, not a single pooled fraction),
> `most_active_hour` (format as a 12-hour time like "8 PM", or "—" when `null`).
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

## Prompt 4 — Blast reply rate

The blast APIs now return how many recipients texted back within 72h of a send
(channel-agnostic — works for both Twilio and SlickText, unlike link CTR):

- `GET /api/blasts` — each draft now includes `reply_rate_pct` (integer 0–100 or
  `null`), `replies` (count), and `reply_recipients` (recipients we can attribute).
- `GET /api/blasts/{id}/status` — same three fields: `reply_rate_pct`, `replies`,
  `reply_recipients`.

> On the Blasts list and the blast detail/results view, show a **"Reply rate"**
> stat next to the existing sent/CTR numbers for any **sent** blast. Render it as
> `{reply_rate_pct}%` with a small subline `{replies} of {reply_recipients}
> replied`. When `reply_rate_pct` is `null` (e.g. an older blast sent before reply
> tracking), show "—" with a tooltip "Not tracked for this blast". Use the
> existing stat-card styling — no new palette. Add a one-line helper near the
> number: "Fans who texted back within 72h of this blast."

---

## Verify after submitting

1. **Custom Links:** inside the Links card, "+ Add link" adds a row (label +
   https URL + when-to-send); save, reload — it persists. A row with a non-`http`
   URL or blank label is dropped on refetch. Adding an 11th link is blocked.
2. **Analytics:** the three hero numbers, the secondary tiles, and all charts
   render from real data; empty states show "—" rather than crashing.
3. **PDF:** "Download PDF" opens the print dialog and the preview shows only the
   report (no sidebar/buttons), on a light background, without cut-off charts.
4. **Blast reply rate:** sent blasts show a "Reply rate" stat (e.g. `67%` ·
   "2 of 3 replied"); blasts without recipient tracking show "—".
5. **Analytics blast stat:** the Analytics report shows a "Blast reply rate" tile
   (`blast_reply_rate`) with a "across N campaigns" subline, or "—" when null.
