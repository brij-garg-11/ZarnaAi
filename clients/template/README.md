# Client #[N] — [Creator Name]

**Status:** Pre-launch  
**Signed:** [date]  
**Platform:** SMS via SlickText  

---

## Who They Are

[1-2 sentences about this creator — who they are, what kind of comedy/content they do, who their fans are.]

---

## Onboarding Checklist

### Step 1 — Content Intake (their time + yours)
- [ ] Get YouTube channel link or specific video list
- [ ] Get podcast RSS feed or episode files
- [ ] Get any specials, book, written material
- [ ] Get 3-5 sentences from them about their voice ("I'm known for being...")
- [ ] Ask what topics they never want the AI to touch
- [ ] Get 5-10 example replies they'd actually send fans (DMs, comments, etc.)

### Step 2 — Voice Config (your work, ~2 hours)
- [ ] Copy `creator_config/TEMPLATE.json` → `creator_config/[slug].json`
- [ ] Write the `style_rules` field after watching/reading their material
- [ ] Fill in `tone_examples` with real fan/reply pairs
- [ ] Fill in `hard_fact_guardrails` — names, family, anything that can't be wrong
- [ ] List `banned_topics` specific to them

### Step 3 — Content Ingestion (your work, ~30 min, mostly automated)
- [ ] Run `python scripts/ingest_youtube.py` (update CHANNEL_ID first)
- [ ] Run `python scripts/ingest_podcast.py` (update RSS URL first)
- [ ] Run `python scripts/ingest_special.py` for any specials
- [ ] Run `python scripts/ingest_book.py` if they have a book
- [ ] Run `python scripts/build_embeddings.py`
- [ ] Verify: `training_data/[slug]_chunks.json` and `[slug]_embeddings.json.gz` exist

### Step 4 — SMS Setup (~20 min)
- [ ] Create SlickText account (or set up under our account)
- [ ] Pick and configure keyword
- [ ] Set webhook URL to point at this creator's Railway deployment
- [ ] Note API key and brand ID for env vars

### Step 5 — Railway Deployment (~10 min)
- [ ] Create new Railway project from our repo
- [ ] Add Postgres database plugin
- [ ] Set all env vars (see below)
- [ ] Deploy both services: main app + operator dashboard
- [ ] Confirm `/health` returns `{"status": "ok"}`

### Step 6 — Bootstrap & Test (~1 hour)
- [ ] Hit `/operator/setup` to create first login
- [ ] Add our team as operators
- [ ] Give creator their login (owner role)
- [ ] Text the number 20+ times — verify voice is right
- [ ] Test keyword signup flow
- [ ] Test STOP / opt-out handling
- [ ] Creator texts it themselves and signs off (document this)

### Step 7 — Go Live
- [ ] Creator announces keyword at their first show / on social
- [ ] Monitor logs and error metrics for 24-48 hours
- [ ] Check dashboard engagement at 48 hours

---

## Deployment Env Vars

```
CREATOR_SLUG=[slug]
DATABASE_URL=[Railway Postgres URL]
SECRET_KEY=[generate: openssl rand -hex 32]

GEMINI_API_KEY=[our key or their own]
OPENAI_API_KEY=[optional]
ANTHROPIC_API_KEY=[optional]

SLICKTEXT_API_KEY=[from their SlickText account]
SLICKTEXT_BRAND_ID=[from their SlickText account]
SLICKTEXT_WEBHOOK_SECRET=[generate: openssl rand -hex 24]

OPERATOR_BOOTSTRAP_EMAIL=[creator's email]
OPERATOR_BOOTSTRAP_PASSWORD=[temp password, they reset after]
```

---

## What We Run for Them

| Layer | Who Owns It |
|---|---|
| AI brain + voice config | Us |
| Training data + embeddings | Us |
| Railway hosting | Us |
| Database (all fan data) | Us |
| SMS pipeline | Us |
| Operator dashboard access | Them (view + sends) |
| Privacy policy | Them |
| Fan opt-in consent | Them |
| Blast content approval | Them |

---

## Content Ingested

| Source | Status | Notes |
|---|---|---|
| YouTube | [ ] Pending | Channel ID: |
| Podcast | [ ] Pending | RSS: |
| Specials | [ ] Pending | |
| Book | [ ] N/A | |

---

## SMS Keywords

| Keyword | Platform | Purpose |
|---|---|---|
| `[KEYWORD]` | SlickText | Main subscriber list |

---

## Voice Config

See `creator_config/[slug].json`

Key notes:
- [brief summary of their voice once written]

---

## Contacts

- **Creator contact:** [name, email, phone]
- **Managed by:** Brij
