# ✅ Blast Issues — Apr 8, 2026
_Completed: Apr 21, 2026_
_Source: `docs/analytics/blast_issues_apr8.md`_

All 9 issues from the Malala X Zarna blast post-mortem resolved:

- **#1** — Blast button lock: send button now disables on first click (`blast.html`) — Apr 21
- **#2** — Post-send confirmation: sent view shows Total/Sent/Failed stat grid (was already done)
- **#3** — Subscriber count: nightly SlickText sync cron live on Railway, contacts table stays current — Apr 21
- **#4** — Blast coverage gap: fixed by #3 — all 5,025 subscribers now in `contacts` table — Apr 21
- **#5** — Duplicate delivery: `get_audience_phones()` uses `DISTINCT` — no double-sends from our side; SlickText-side cross-textword behavior is out of our control
- **#6** — STOP guardrail: hard guardrail always says "reply STOP" (commit `f055b6a`)
- **#7** — Reply attribution timestamp: uses `COALESCE(started_at, sent_at)` (commit `71f431b`)
- **#8** — Auto-refresh default: defaults to on, persisted in `localStorage` (`admin/__init__.py`) — Apr 21
- **#9** — CTR denominator: Insights uses `sent_count` not inflated `sent_to`
