# Zarna AI — Scale, Security & Infrastructure Report
*As of March 28, 2026*

---

## 1. What We Can Handle Right Now

### Concurrent Users
- **Webhook ingestion**: Effectively unlimited — webhooks return in <50ms, limited only by Railway's network
- **AI processing (Gemini)**: ~480 messages per 60-second spike window (1,000 RPM limit / 2 calls per message)
- **Comfortable sustained load**: ~200-300 messages per minute without any delays
- **Realistic show night**: A few hundred people texting over a 5-10 minute window — no problem at all

### Infrastructure Capacity

| Component | Current Limit | Current Usage |
|---|---|---|
| Gunicorn workers | 8 | — |
| AI thread pool | 32 concurrent threads | — |
| Gemini Flash RPM | 1,000 | ~5 (0.5%) |
| Gemini Embedding RPM | 3,000 | ~3 (0.1%) |
| Postgres connections | 10 per worker (80 total) | — |
| Railway Hobby plan | 8GB RAM / shared CPU | Low |

### What Happens If Limits Are Exceeded
- Gemini rate limit hit → bot sends a funny fallback message ("My brain went on vacation…") instead of crashing
- Per-phone rate limit (3 msgs/60s) prevents any single person from spamming
- Webhooks always return 200/204 instantly — SlickText/Twilio never retry or error out

---

## 2. How to Scale Further

### For a Large Show (1,000–5,000 simultaneous texters)

| What to do | Cost | Time to set up |
|---|---|---|
| Request Gemini RPM increase to 5,000 | Free | 24-48hrs (Google approval) |
| Upgrade Railway to Pro plan | ~$20/mo | Instant |
| Increase Postgres connection pool (maxconn) | Free (code change) | 5 mins |

**Gemini quota increase is the single most important step.** Request it at
[aistudio.google.com](https://aistudio.google.com) → Rate Limit → "Request increase."
Ask for 5,000 RPM citing live event audience participation.

### For Massive Scale (10,000+ users, ongoing daily traffic)

| What to do | Cost | Notes |
|---|---|---|
| Move to Railway Pro + autoscaling | ~$50-100/mo | Auto-adds workers under load |
| Replace in-process dedup/rate-limit with Redis | ~$15/mo on Railway | Makes dedup bulletproof across workers |
| Add a message queue (e.g. Railway + Redis Queue) | Included with Redis | Smooths out spike bursts instead of dropping them |
| Move embeddings to a vector database (e.g. Pinecone) | ~$70/mo | Needed if training data grows beyond ~50k chunks |
| Dedicated Gemini project with higher tier | Pay-as-you-go | Auto-scales with usage, no fixed cost |

**Rough cost at 10k daily active users: ~$150-200/month total**
(Railway + Redis + Gemini usage)

---

## 3. Security

### What Is Locked Down

| Layer | Protection | Status |
|---|---|---|
| `/message` test endpoint | `API_SECRET_KEY` required in production; `X-Api-Key` must match (constant-time) | ✅ Active |
| SlickText webhook | Optional `SLICKTEXT_WEBHOOK_SECRET` + header `X-Zarna-Webhook-Secret` | ✅ When set |
| Twilio webhook | Cryptographic signature validation on every request | ✅ Active |
| All endpoints | Per-phone/IP rate limiting (3 req/60s) | ✅ Active |
| Duplicate messages | Dedup by MessageSid / ChatMessageId | ✅ Active |
| Database | Internal to Railway network, not publicly accessible | ✅ |
| API keys & secrets | Environment variables only, never in code or GitHub | ✅ |
| SQL queries | Parameterized throughout — no injection risk | ✅ |
| Repository | Private on GitHub | ✅ |

### Known Limitations (Low Risk)
- **SlickText webhook** — No provider-signed payloads. Mitigation: set `SLICKTEXT_WEBHOOK_SECRET` and send
  `X-Zarna-Webhook-Secret` from SlickText **if** their dashboard supports custom webhook headers; otherwise
  keep the URL private and rely on rate limits + logging.
- **Dedup cache is per-worker** — in theory, a duplicate message could slip through if two
  different workers handle the same retry simultaneously. Extremely rare and harmless in practice.
  Would be solved by moving dedup to Redis at scale.
- **No phone number encryption** — phone numbers are stored as plain text in Postgres. Standard
  for SMS apps at this scale, but worth noting if strict PII compliance is ever required.

### Data Stored
- `contacts` table: phone number, source, signup timestamp
- `messages` table: phone number, message text, bot reply, timestamp
- Nothing else — no names, no payment info, no personal details beyond what users text in

---

## Summary

| Scenario | Ready? | Action needed |
|---|---|---|
| Show night, hundreds of texters | ✅ Yes | None |
| Large show, 1,000-5,000 texters | ⚠️ Mostly | Request Gemini quota increase (free) |
| Ongoing daily traffic at scale | 🔧 Needs work | Redis + Railway Pro (~$150/mo) |

**Before any big show:** Request Gemini quota increase to 5,000 RPM at
[aistudio.google.com](https://aistudio.google.com) → Rate Limit → "Request increase."
It's free and takes 1-2 days to get approved.
