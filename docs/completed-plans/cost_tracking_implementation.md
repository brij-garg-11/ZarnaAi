# Exact Cost Tracking — Implementation Plan
_To be built. Every file, every change, exact line count._
_Written: Apr 21, 2026_

---

## Goal

Track exactly what we spend per client per month — down to fractions of a cent — so the Notion CRM shows real numbers, not estimates.

**Three cost sources:**
1. **AI tokens** — captured at generation time from the API response (Gemini, OpenAI, Anthropic)
2. **SMS costs** — pulled nightly from Twilio's own billing API (same numbers as your invoice)
3. **Phone rental** — $1.15/month per Twilio number (already tracked)

---

## Why We're Estimating Today (The Problem)

`notion_crm.py` currently does this:

```python
est_ai_cost  = round(msgs_month * 0.004, 2)   # flat blended rate — wrong
est_sms_cost = round(msgs_month * 0.0079, 2)  # flat rate — misses inbound/outbound split
```

And `generator.py` does this:

```python
response = _CLIENT.models.generate_content(model=..., contents=prompt)
return (response.text or "").strip()   # token counts are IN response — thrown away every time
```

All three APIs (Gemini, OpenAI, Anthropic) return exact token counts in every response. We discard them on every single call.

---

## Part 1 — AI Token Cost Capture

### Change 1: `app/brain/generator.py`

**Add token pricing constants at the top of the file:**

```python
# Per-token pricing (USD per token, as of Apr 2026)
_TOKEN_PRICES = {
    "gemini":    {"input": 0.10 / 1_000_000,  "output": 0.40 / 1_000_000},
    "openai":    {"input": 0.15 / 1_000_000,  "output": 0.60 / 1_000_000},
    "anthropic": {"input": 3.00 / 1_000_000,  "output": 15.00 / 1_000_000},
}

def calc_ai_cost(provider: str, prompt_tokens: int, completion_tokens: int) -> float:
    p = _TOKEN_PRICES.get(provider, _TOKEN_PRICES["gemini"])
    return round(prompt_tokens * p["input"] + completion_tokens * p["output"], 8)
```

**Change `_generate_gemini_raw` to return a tuple instead of a string:**

```python
# Before:
def _generate_gemini_raw(prompt: str) -> str:
    response = _CLIENT.models.generate_content(model=GENERATION_MODEL, contents=prompt)
    return (response.text or "").strip()

# After:
def _generate_gemini_raw(prompt: str) -> tuple[str, int, int]:
    response = _CLIENT.models.generate_content(model=GENERATION_MODEL, contents=prompt)
    text = (response.text or "").strip()
    usage = response.usage_metadata or {}
    prompt_tok = getattr(usage, "prompt_token_count", 0) or 0
    output_tok = getattr(usage, "candidates_token_count", 0) or 0
    return text, prompt_tok, output_tok
```

**Same change for `_generate_openai_raw`:**
```python
# After:
def _generate_openai_raw(prompt: str) -> tuple[str, int, int]:
    ...
    return text, r.usage.prompt_tokens, r.usage.completion_tokens
```

**Same change for `_generate_anthropic_raw`:**
```python
# After:
def _generate_anthropic_raw(prompt: str) -> tuple[str, int, int]:
    ...
    return text, msg.usage.input_tokens, msg.usage.output_tokens
```

**Change `_produce_raw_text` to return `tuple[str, str, int, int]`** (text, provider, prompt_tokens, completion_tokens) — bubble the tuple up through all the fallback paths.

**Change `generate_zarna_reply` to return `tuple[str, str, int, int]`** instead of `str`.

~30 lines total changed.

---

### Change 2: `app/brain/handler.py`

Unpack the new tuple from `generate_zarna_reply` and calculate cost:

```python
# Before (line ~196):
reply = generate_zarna_reply(...)

# After:
reply, ai_provider, prompt_tokens, completion_tokens = generate_zarna_reply(...)
ai_cost = calc_ai_cost(ai_provider, prompt_tokens, completion_tokens)
```

Add the 4 new fields to the `save_reply_context_async` call (~line 245):
```python
save_reply_context_async(
    ...existing args...,
    provider=ai_provider,
    prompt_tokens=prompt_tokens,
    completion_tokens=completion_tokens,
    ai_cost_usd=ai_cost,
)
```

~10 lines changed.

---

### Change 3: `app/analytics/outcome_scorer.py`

Add 4 new optional params to `save_reply_context_async` and pass them through to `storage.save_reply_context`:

```python
def save_reply_context_async(
    executor, storage, message_id, reply_text, intent, tone_mode,
    routing_tier, gen_ms, conversation_turn, sell_variant=None,
    # NEW:
    provider=None, prompt_tokens=None, completion_tokens=None, ai_cost_usd=None,
) -> None:
```

~8 lines changed.

---

### Change 4: `app/storage/base.py`

Add the 4 new params (with `None` defaults) to the `save_reply_context` signature. Backwards compatible — nothing breaks.

~6 lines changed.

---

### Change 5: `app/storage/postgres.py`

**Add 4 migration lines to `_ENGAGEMENT_ANALYTICS_MIGRATIONS`:**

```python
"ALTER TABLE messages ADD COLUMN IF NOT EXISTS provider          TEXT",
"ALTER TABLE messages ADD COLUMN IF NOT EXISTS prompt_tokens     INT",
"ALTER TABLE messages ADD COLUMN IF NOT EXISTS completion_tokens INT",
"ALTER TABLE messages ADD COLUMN IF NOT EXISTS ai_cost_usd       NUMERIC(10,8)",
```

**Extend the `UPDATE messages SET ...` query in `save_reply_context` to include the 4 new columns.**

~12 lines changed.

---

### Result of Part 1

Every assistant message row now has:

| Column | Example |
|---|---|
| `provider` | `"gemini"` / `"openai"` / `"anthropic"` |
| `prompt_tokens` | `847` |
| `completion_tokens` | `94` |
| `ai_cost_usd` | `0.00012160` |

Query to get exact AI cost per client per month:

```sql
SELECT
  c.creator_slug,
  SUM(m.ai_cost_usd)        AS total_ai_cost,
  COUNT(*)                  AS message_count,
  SUM(m.prompt_tokens)      AS total_prompt_tokens,
  SUM(m.completion_tokens)  AS total_output_tokens,
  m.provider,
  COUNT(*) FILTER (WHERE m.provider = 'gemini')    AS gemini_msgs,
  COUNT(*) FILTER (WHERE m.provider = 'openai')    AS openai_msgs,
  COUNT(*) FILTER (WHERE m.provider = 'anthropic') AS anthropic_msgs
FROM messages m
JOIN contacts c ON c.phone_number = m.phone_number
WHERE m.role = 'assistant'
  AND m.created_at >= DATE_TRUNC('month', NOW())
GROUP BY c.creator_slug, m.provider;
```

---

## Part 2 — SMS Cost Capture (Twilio Billing API)

### New File: `scripts/sync_twilio_costs.py`

Runs nightly via Railway cron. Calls Twilio's Usage Records API — the same data source as your Twilio invoice — and stores it per creator per day.

```
What it does each night:
  1. For each day since last sync (or last 30 days on first run):
     GET https://api.twilio.com/2010-04-01/Accounts/{SID}/Usage/Records/Daily
       ?StartDate=YYYY-MM-DD&EndDate=YYYY-MM-DD&Category=sms
  2. Response includes: count + price (USD) for inbound and outbound per number
  3. Map each phone number → creator_slug via DB lookup
  4. INSERT INTO sms_cost_log ... ON CONFLICT DO UPDATE
```

**New DB table** (add to `postgres.py` migrations):

```sql
CREATE TABLE IF NOT EXISTS sms_cost_log (
    id                SERIAL PRIMARY KEY,
    log_date          DATE NOT NULL,
    creator_slug      TEXT NOT NULL,
    phone_number      TEXT NOT NULL,
    inbound_count     INT  DEFAULT 0,
    outbound_count    INT  DEFAULT 0,
    inbound_cost_usd  NUMERIC(10,4) DEFAULT 0,
    outbound_cost_usd NUMERIC(10,4) DEFAULT 0,
    synced_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (log_date, phone_number)
);
```

**Deploy as Railway cron** — add `railway.sync_twilio_costs.toml`:
```toml
[deploy]
startCommand = "python scripts/sync_twilio_costs.py"
cronSchedule = "0 2 * * *"   # 2am UTC nightly
```

~80 lines total.

---

## Part 3 — Cost Summary Endpoint

### New route in `operator/app/routes/api.py`

`GET /api/billing/cost-breakdown?slug=zarna&month=2026-04`

Response shape:
```json
{
  "slug": "zarna",
  "month": "2026-04",
  "ai": {
    "total_usd": 18.42,
    "by_provider": {
      "gemini": 14.10,
      "openai": 3.21,
      "anthropic": 1.11
    },
    "message_count": 4821,
    "prompt_tokens": 3840000,
    "completion_tokens": 482000
  },
  "sms": {
    "total_usd": 38.14,
    "inbound_count": 2100,
    "outbound_count": 4821,
    "inbound_cost_usd": 17.85,
    "outbound_cost_usd": 20.29
  },
  "phone_rental": 1.15,
  "total_cost_usd": 57.71,
  "monthly_fee": 0,
  "net_margin": -57.71
}
```

~50 lines.

---

## Part 4 — Wire Notion CRM to Real Numbers

### Change 6: `operator/app/notion_crm.py`

`sync_customer_costs` currently estimates. Replace estimates with DB queries:

```python
# Before:
est_ai_cost  = round(msgs_month * AI_COST_PER_MSG, 2)
est_sms_cost = round(msgs_month * SMS_COST_PER_MSG, 2)

# After:
cur.execute(
    """SELECT COALESCE(SUM(ai_cost_usd), 0) FROM messages m
       JOIN contacts c ON c.phone_number = m.phone_number
       WHERE c.creator_slug = %s AND m.role = 'assistant'
         AND m.created_at >= DATE_TRUNC('month', NOW())""",
    (slug,)
)
est_ai_cost = float(cur.fetchone()[0])

cur.execute(
    """SELECT COALESCE(SUM(inbound_cost_usd + outbound_cost_usd), 0)
       FROM sms_cost_log
       WHERE creator_slug = %s
         AND log_date >= DATE_TRUNC('month', CURRENT_DATE)""",
    (slug,)
)
est_sms_cost = float(cur.fetchone()[0])
```

~15 lines changed.

---

## Full Scope Summary

| File | Type | Change | Est. Lines |
|---|---|---|---|
| `app/brain/generator.py` | Modify | 3 generators return token tuples; add pricing constants + `calc_ai_cost` | ~30 |
| `app/brain/handler.py` | Modify | Unpack tuple; pass 4 new fields to scorer | ~10 |
| `app/analytics/outcome_scorer.py` | Modify | 4 new params on `save_reply_context_async` | ~8 |
| `app/storage/base.py` | Modify | 4 new params on `save_reply_context` | ~6 |
| `app/storage/postgres.py` | Modify | 4 migration columns; extend UPDATE query; add `sms_cost_log` table | ~18 |
| `operator/app/notion_crm.py` | Modify | Pull from DB instead of estimating | ~15 |
| `scripts/sync_twilio_costs.py` | **New** | Nightly Twilio billing sync | ~80 |
| `operator/app/routes/api.py` | Modify | Add `GET /api/billing/cost-breakdown` route | ~50 |
| `operator/railway.sync_twilio_costs.toml` | **New** | Railway cron config | ~5 |
| **Total** | | | **~222 lines** |

---

## Important Notes

- **Zero breaking changes.** All 4 new message columns are nullable. Old rows without token data still work everywhere.
- **Token pricing will drift.** The `_TOKEN_PRICES` dict in `generator.py` needs to be updated when providers change prices. Consider moving to a config file or env var later.
- **SlickText costs ($750/mo flat)** can only be apportioned by message share, not measured per-message. That's a manual line item in the CRM — not worth automating.
- **Railway hosting costs** are shared infrastructure — same situation, manual apportionment.
- **Twilio sync script needs `TWILIO_ACCOUNT_SID` and `TWILIO_AUTH_TOKEN`** in env — both already present for SMS sending, just need to be confirmed accessible in the cron service.
- **Build Part 1 first** — it's the biggest unknown cost and requires zero new infrastructure. Parts 2–4 build on top.
