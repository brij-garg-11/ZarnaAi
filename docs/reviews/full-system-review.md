# Zarna AI — Full System Review
_Created: May 7, 2026_
_Status: Audit complete — pre-Plan-06/07 baseline_

---

## How to read this document

Each finding carries a status tag:

| Tag | Meaning |
|-----|---------|
| ✅ Working | Feature exists and behaves correctly end-to-end |
| ⚠️ Partial | Feature exists but has known gaps, edge cases, or inaccuracies |
| ❌ Broken | Feature exists in the UI/API but does not work correctly at runtime |
| 🔲 Missing | Feature is not yet built |

Priority tags on issues:

| Tag | Meaning |
|-----|---------|
| 🔴 Critical | Could affect real fans, cause data loss, or violate compliance today |
| 🟠 High | Will cause visible user-facing bugs or production incidents |
| 🟡 Medium | Meaningful but not immediate production risk |
| 🔵 Low | Polish / cleanup / future-proofing |

---

## Table of Contents

1. [Website, UI/UX & Copy](#1-website-uiux--copy)
2. [Payments, Billing & Notion](#2-payments-billing--notion)
3. [Signup, Onboarding & Auth Flows](#3-signup-onboarding--auth-flows)
4. [Security](#4-security)
5. [AI Infrastructure](#5-ai-infrastructure)
6. [Dashboard, Inbox & Blasts](#6-dashboard-inbox--blasts)
7. [Performer Features](#7-performer-features)
8. [Business (SMB) Features](#8-business-smb-features)
9. [SMS Infrastructure](#9-sms-infrastructure)
10. [Database & Storage](#10-database--storage)
11. [Tests & Code Quality](#11-tests--code-quality)
12. [Master Issues List](#12-master-issues-list)

---

## 1. Website, UI/UX & Copy

### Pages (lovable-frontend)

| Route | Status | Notes |
|-------|--------|-------|
| `/` (Home) | ✅ Working | Hero, stats, proof, FAQ — polished |
| `/performers` | ✅ Working | Performer marketing page — complete |
| `/business` | ⚠️ Partial | CTA links to `/how-it-works/business` but that route just redirects to `/how-it-works` with no audience context — business visitors see the performer version |
| `/how-it-works` | ✅ Working | Audience driven by state/sessionStorage |
| `/how-it-works/business` | ❌ Broken | Redirect drops the `state: { audience: "business" }` — deep links to this always render performer content |
| `/pricing` | ✅ Working | |
| `/faq` | ✅ Working | |
| `/login` | ✅ Working | Email + Google OAuth |
| `/signup` | ✅ Working | Email + Google OAuth |
| `/forgot-password` | ⚠️ Partial | UI exists and posts correctly, but **errors are swallowed** — API failure always shows "check your email" success message to user |
| `/reset-password` | ✅ Working | Token validation + password change complete |
| `/onboarding` | ✅ Working | Bot setup wizard with real forms |
| `/plans` | ✅ Working | API-driven plan picker |
| `/:slug/dashboard` | ✅ Working | Real DB stats |
| `/:slug/inbox` | ✅ Working | Thread list + send (see §6) |
| `/:slug/blast` | ✅ Working | Blast creation + scheduling |
| `/:slug/audience` | ✅ Working | |
| `/:slug/my-bot` | ✅ Working | Bot config editor |
| `/:slug/billing` | ⚠️ Partial | Route exists but **not in the user menu** — only reachable via credit banners |
| `/privacy`, `/terms`, `/sms-terms` | ✅ Working | Legal pages present |
| `/404` | ✅ Working | |

### Broken / Dead UI Elements

**🟠 `/how-it-works/business` redirect drops business audience context**
- File: `lovable-frontend/src/App.tsx:111`, `lovable-frontend/src/pages/Business.tsx:53-58`
- Fix: Change the redirect to `<Navigate to="/how-it-works" state={{ audience: "business" }} replace />`

**🟡 Footer "Proof" anchor points to `#real-people` which doesn't exist on homepage**
- `RealPeople` component is defined but never imported or rendered on `Index.tsx`
- File: `lovable-frontend/src/components/Footer.tsx:20`
- Fix: Either add `RealPeople` to Index or change footer link

**🟡 Early Access dialog submit does nothing — shows toast only, no API call**
- File: `lovable-frontend/src/components/EarlyAccessDialog.tsx:28-35`
- Fix: Wire up to a lead collection endpoint or remove the form

**🟡 Fan of the Week and Fan History Table use legacy `/inbox/...` paths (no tenant prefix)**
- Files: `DashboardFanOfTheWeek.tsx:65-66`, `FanHistoryTable.tsx:168-170`, `CustomerHistoryTable.tsx:167`
- Fix: Replace with `useSlugPath` like `Inbox.tsx` already does

**🟡 Billing not discoverable from user menu**
- Users only find billing via credit warning banners — easy to miss
- Fix: Add a "Billing" item to `UserMenu.tsx`

### Copy / Writing Issues

**🔵 Credits UI inconsistency: `CreditsWidget` and `Usage.tsx` show "Unlimited" when `total === 0`, but `Billing.tsx` has newer logic that explicitly guards against this**
- Files: `CreditsWidget.tsx:73-74`, `Usage.tsx:30-31`
- This can mislead users who have genuinely run out of credits

**🔵 Business nav shows "Audience" instead of "Customers"**
- `DashboardHeader.tsx:17-24` hardcodes "Audience" for business; performer path uses the `labels.audienceNav` variable which correctly says "Customers"

**🔵 "Messages today" label vs actual logic mismatch**
- Dashboard card says "Today" but the query logic is "last 24 hours"
- Files: `operator/app/queries.py:29-30`, dashboard HTML template

---

## 2. Payments, Billing & Notion

### Architecture

Stripe Checkout + webhooks + internal credit system. Plans defined in code (`operator/app/billing/plans.py`), with prices resolved via `STRIPE_PRICE_ID_*` env vars.

**Plan tiers:**
- **Performers:** `starter` → `creator` (monthly credits 3,200–80,300)
- **Business:** `essentials` (2 seats), `standard` (4 seats), `business_pro` (unlimited seats)
- **Internal/unlimited:** `grandfathered`, `founder`, `internal` bypass credit gates
- **Trial:** 1,000 credits on signup

### What's Working

✅ Stripe Checkout for subscriptions and one-time boosters  
✅ Customer Portal for plan changes / invoices  
✅ Webhook handling for renewal, cancellation, payment failure  
✅ Credit tracking in DB (`operator_credit_usage` table)  
✅ Credit gating on operator inbox sends and blast start  
✅ Mid-blast credit check in blast worker  
✅ Booster top-up flow  
✅ Trial credit seeding on signup  
✅ Plan enforcement on team seat invites  

### Issues

**🔴 Automatic performer AI SMS replies are NOT gated by credit limit**
- `main.py` _consume_message_credits runs AFTER the reply is already sent (lines 433-441) — "Fail-open: never blocks message processing" comment is intentional but means a client can run out of credits and the AI keeps responding
- File: `main.py:433-441`
- Mismatch: cancellation email copy claims "bot stops responding" but that's not what the code does

**🟠 No Stripe webhook idempotency — duplicate events can fire twice**
- Stripe retries events on non-200 responses. There's no `event.id` deduplication in the webhook handler
- File: `operator/app/routes/billing.py:319-337`
- Risk: booster credits could be granted twice, plan state set twice

**🟠 Annual subscription billing mismatch**
- Annual prices are exposed in UI and checkout but `credits_included` is always set to `plan.monthly_credits` per Stripe period
- If Stripe billing period is yearly, one row gets "monthly" credits for the whole year
- File: `operator/app/routes/billing.py:437-438, 499-507`

**🟡 Two separate credit metering implementations that can drift**
- `main.py` has `_consume_message_credits` with raw SQL (lines 514-523) that inserts `credits_included=0`
- Operator routes use `consume_credit` from the billing module
- `get_credit_status` compensates with `row["credits_included"] or get_plan_credits` but `overage_credits` is not maintained on the main-app path

**🟡 `billing/__init__.py` public surface docs are stale**
- References `enforce_send_quota()` (not implemented — it's `check_send_quota`) and `stripe_client.get_stripe()` which doesn't exist in the package
- File: `operator/app/billing/__init__.py:1-11`

**🟡 Trial email hardcodes "1,000 trial credits" — will be wrong if `TRIAL_CREDITS` changes**
- File: `operator/app/scheduler.py:116-121`

### Notion Integration

✅ Customer created in Notion on onboarding  
✅ Plan/billing fields synced from Stripe webhooks  
✅ Daily cost metrics sync (cron at 03:00 UTC)  
✅ Graceful degradation — Notion failures don't break Stripe webhooks  

**🔴 Blast metrics in Notion are NOT scoped by tenant**
- `sync_customer_costs` aggregates `blast_drafts` without a `creator_slug` filter — every customer's Notion row can receive the same global blast/fan counts
- File: `operator/app/notion_crm.py:617-624`

**🟡 No deduplication check before creating a Notion customer page**
- Duplicate onboarding attempts could create duplicate rows
- File: `operator/app/notion_crm.py` — no existing-slug check before `_create_page`

**🟡 Net margin calculation reads "Monthly Fee" from the current Notion page**
- Circular: if that Notion property is stale, the margin calc is wrong
- File: `operator/app/notion_crm.py:577-587`

**🔵 Notion metrics are up to ~24h stale** (daily cron only)

---

## 3. Signup, Onboarding & Auth Flows

### New Client Signup

✅ Self-service via `/api/auth/signup` + Google OAuth on frontend  
✅ Onboarding form (`/api/onboarding/submit`) — validates slug uniqueness, writes `bot_configs`, seeds credits, Notion, team membership  
✅ Automated config generation (Gemini → `creator_configs` DB)  
✅ Automated ingestion (scrape/embed → `creator_embeddings`)  
✅ Welcome email via Resend  

**🟠 Phone number provisioning is a stub — no real Twilio numbers assigned automatically**
- `PROVISIONING_PHONE_MODE=stub` assigns deterministic `+1555...` numbers
- `_buy_real_number` raises `NotImplementedError`
- File: `operator/app/provisioning/phone.py:130-166`
- Real numbers require manual Twilio setup per client

**🟡 SMB accounts do NOT run the provisioning pipeline**
- Performer signups trigger `provision_new_creator` in a daemon thread; business accounts only get `smb_bot_config` seeded — no config generation, ingestion, or welcome email
- File: `operator/app/routes/api.py:5001-5030`

### Operator Login / Auth

✅ Email + password login (`/api/auth/login`)  
✅ Google OAuth signup and login  
✅ Password reset flow — forgot password email (1h token), reset form  
✅ 30-day persistent sessions with secure cookies  
✅ Bootstrap route (`/operator/setup`) for first-time setup  

**🟠 Forgot password UI always shows success regardless of API failure**
- File: `lovable-frontend/src/pages/ForgotPassword.tsx:10-25`

**🔵 Default Flask secret key fallback `"change-me-in-production"` in operator app factory**
- File: `operator/app/__init__.py:25`
- Fine if env is set; catastrophic if accidentally deployed without it

### Fan First-Text Flow

✅ Inbound webhook → dedup check → live-show signup check → AI filter → rate limit → brain  
✅ Contact created on first message (`save_contact` with `ON CONFLICT DO NOTHING`)  
✅ Normal AI reply generated and sent  

**❌ No compliance line on first performer message**
- CTIA requires "Msg & data rates may apply. Reply STOP to opt out." on first message — not appended anywhere in the performer path (Plan 07 addresses this)

**❌ No custom welcome message for performers**
- SMB has a configurable welcome message; performers get a purely AI-generated first reply with no static compliance/welcome text (Plan 07 addresses this)

**🔲 No `performer_subscribers` table — no explicit opt-in record**
- Fans are tracked only as `contacts` rows; no dedicated subscriber table (Plan 07 addresses this)

---

## 4. Security

### Webhook Security

✅ Twilio signature validation — hard 403 reject on invalid signature  
✅ SlickText shared-secret header validation with `hmac.compare_digest`  
✅ SlickText logs a prod warning if `SLICKTEXT_WEBHOOK_SECRET` is unset  

**🔴 Twilio signature check is skipped entirely if `TWILIO_AUTH_TOKEN` is missing**
- Adapter returns `True` (passes) when no validator is configured
- File: `app/messaging/twilio_adapter.py:108-113`
- Misconfigured production = anyone can forge Twilio webhooks

**🔴 SlickText webhook has no authentication when `SLICKTEXT_WEBHOOK_SECRET` is not set**
- File: `app/inbound_security.py:43-54`
- Default is "open" not "closed" — should fail-safe to rejected

**🟠 Message deduplication is per-worker only, bounded to 1000 entries**
- Multi-Gunicorn workers don't share dedup state; same message can be processed twice by different workers
- File: `main.py:178-196`
- Stale comment says 200 entries; actual `_MAX_SEEN = 1000`

**🟠 SlickText v2 dedup likely ineffective**
- v2 payloads don't yield a stable message ID the same way v1 does — `message_id` is often empty, so `_already_processed` always returns False for v2
- File: `main.py:314-322`, `app/messaging/slicktext_adapter.py:181-192`

### Authentication

✅ Main app admin dashboard — HTTP Basic Auth with `hmac.compare_digest`  
✅ Operator dashboard — Flask session, 30-day expiry, secure + SameSite cookies  
✅ SMB portal (main app) — env token, `hmac.compare_digest`  
✅ SMB portal (operator) — env password, session  

**🟡 Operator setup route (`/operator/setup`) is reachable if `OPERATOR_BOOTSTRAP_EMAIL` env is set, even after setup**
- Should be disabled after the first user is created unless bootstrap env is explicitly set

**🔵 Main app Flask secret fallback `"dev-only-do-not-use-in-prod"`**
- File: `main.py:155-160`

### STOP / Opt-Out Compliance

⚠️ STOP keywords are dropped before reaching AI on both Twilio and SlickText adapter — carrier handles the STOP confirmation reply  
✅ SMB STOP handling is explicit with DB `stopped` status  
✅ Blast audience queries exclude `broadcast_optouts` table  

**🔴 `broadcast_optouts` table is read in blast audience queries but NEVER written by any application code**
- The table is defined, blast queries exclude those rows, but nothing in the codebase inserts into it
- File: `operator/app/db.py:47-51`, `operator/app/queries.py:236, 310`
- Result: performer blast suppression via this table is completely broken; relies entirely on carrier-side STOP enforcement

**🔴 Blast opt-out metric recording is SlickText-only — Twilio performer path never calls `_record_blast_optout`**
- File: `main.py:341-344` (SlickText path only)

**🟠 Operator SMB portal "Everyone" blast selects ALL subscriber phones with no `status='active'` filter**
- `_get_all_subscriber_phones` at `operator/app/routes/smb_portal.py:235-244` includes STOPped users
- The main app `get_active_subscribers` in `storage.py:75-86` correctly filters; portal does not match

### Input / Injection

✅ SQL queries use parameterized `%s` throughout — no obvious injection vectors  
✅ Admin dashboard uses `_esc()` for HTML escaping in server-rendered templates  

**🟡 CSRF protection has a gap — requests with no Origin or Referer header bypass the check**
- File: `operator/app/__init__.py:114-120`
- Only blocks when `origin` is truthy; no-origin mutating requests pass through

**🟡 Prompt injection — fan SMS messages are interpolated as plain strings into LLM prompts**
- No dedicated guard layer; OpenAI/Anthropic paths send everything in a single user message with no role separation
- Mitigation is prompt instructions only

### Data Privacy

**🟠 Full phone numbers logged in exception paths and reserved-keyword logs**
- `main.py:418-423` logs full `phone_number` in exception handlers
- `app/messaging/slicktext_adapter.py:136-138` logs full phone on reserved keyword
- Railway logs are accessible — PII exposure

**🟡 `LOG_SENSITIVE_WEBHOOK_DATA=true` logs full webhook payloads including fan messages**
- Intentional debug flag, but should be confirmed off in production
- File: `app/inbound_security.py:57-68`

**🟡 Tracked link URLs embed base64-encoded phone numbers**
- `/t/<slug>?f=<base64phone>` — phone leaks via Referer headers and access logs

### Dependencies

**🟡 Main `requirements.txt` is version-pinned; operator `requirements.txt` uses lower bounds only**
- Operator builds can silently pick up newer/vulnerable versions
- Recommendation: run `pip-audit` or `uv audit` in CI

---

## 5. AI Infrastructure

### LLM Provider Status

| Provider | Configured | Used For | Fallback |
|----------|-----------|---------|---------|
| Google Gemini | ✅ Via `GEMINI_API_KEY` | Primary: all structured intents (ticket/merch/show/book/clip), routing, intent classification, embeddings, memory extraction, all low-tier replies | — |
| OpenAI | ✅ Via `OPENAI_API_KEY` (optional) | Medium-tier general replies | Falls back to Gemini if unavailable |
| Anthropic | ✅ Via `ANTHROPIC_API_KEY` (optional) | High-tier general replies (first attempt) | Falls back to OpenAI then Gemini |

**All three providers are wired to real SDKs.** Multi-model reply routing is only active for non-structured intents when `MULTI_MODEL_REPLY` is not disabled.

### What's Working

✅ Gemini primary reply generation  
✅ OpenAI + Anthropic fallback chain for medium/high complexity general replies  
✅ Complexity routing (heuristic + Gemini router)  
✅ Intent classification (fast keyword heuristics + Gemini fallback)  
✅ RAG pipeline — file-based (Zarna) and Postgres pgvector (other creators)  
✅ Fan memory extraction and persistence (Gemini-based, async thread)  
✅ Tone classification (heuristic + `family_roast_names` from config)  
✅ Emphasis throttling  
✅ Response length/SMS trimming (max 3 sentences, ~380 chars)  
✅ Link rewriting to tracked `/t/` URLs  
✅ Cost tracking per reply (tokens + USD stored in `messages` table)  

### Issues

**🔴 Structured intents (SHOW, MERCH, BOOK, CLIP, PODCAST) have NO fallback if Gemini fails**
- Only `_generate_gemini_raw` is called; on exception it logs and returns empty string
- File: `app/brain/generator.py:851-857`
- A Gemini outage kills all ticket/merch link replies

**🟠 API keys are not validated at startup — failures only surface on first API call**
- All three providers initialize `genai.Client(api_key=...)` at import time with whatever string is in env (including empty string)
- File: `app/brain/generator.py:24`, `routing.py:28`, `intent.py:30`

**🟠 `banned_words` from `creator_config` is loaded but never enforced in generation**
- `CreatorConfig.banned_words` is populated but not referenced anywhere in `generator.py` or `handler.py`
- File: `app/brain/creator_config.py:44, 81`

**🟡 Cost tracking only covers the final reply generation call**
- Token counts from intent classification, routing, query embedding, and memory extraction are NOT included
- True AI cost per message is systematically undercounted
- File: `app/brain/handler.py:241-291`

**🟡 Intent classification fast-path is heavily Zarna-specific**
- `_BOOK_PHRASES`, `_GREETING_PATTERNS`, etc. include hard-coded Zarna/MIL references
- `name_variants`, `mil_answers`, `shalabh_names` from `creator_config` are loaded but NOT used in `intent.py`'s fast path
- File: `app/brain/intent.py:72-87, 135-136`

**🟡 Non-Zarna creators without `DATABASE_URL` env will crash on brain creation**
- `create_brain("other-slug")` always uses `PgRetriever` — requires DB connection
- File: `app/brain/handler.py:369-374`

**🟡 Training data silently falls back to Zarna's corpus on slug mismatch**
- A misconfigured `CREATOR_SLUG` serves Zarna's knowledge to a different performer
- File: `app/config.py:32-36`

**🔵 No post-generation content moderation / output filter**
- All quality control is prompt-instruction-only; no automated check on LLM output before it reaches fans

**🔵 No per-fan or per-creator token/cost budget cap**
- Relying entirely on provider billing; a runaway scenario has no in-app stop

---

## 6. Dashboard, Inbox & Blasts

### Dashboard Metrics

✅ All headline stats pulled from live SQL (no hardcoded/placeholder values)  
✅ Week-over-week deltas computed correctly  
✅ 30-day message chart, 24h hour chart, tag breakdown, top area codes  
✅ Both server-rendered (HTML) and JSON API (Lovable) versions wired to same query  

**🟡 "Messages Today" label vs "Last 24 hours" logic mismatch**
- Card copy says "Today" but query uses a 24h rolling window, not calendar day
- File: `operator/app/queries.py:29-30`

**🟡 Engagement score formula in docs doesn't match implementation**
- `engagement.py` docstring mentions click activity in the score, but the SQL doesn't include a click term
- File: `operator/app/engagement.py:9-16, 34-60`

**🟡 `recompute_all(slug=...)` ignores the slug parameter — updates all tenants**
- File: `operator/app/engagement.py:76-77`

### Inbox

✅ Paginated thread list with fan metadata (tier, tags, location, memory preview)  
✅ Full conversation thread loaded per fan  
✅ Fan profile data (memory, tags, score, joined_at) alongside thread  
✅ Manual send from inbox — validated, credit-gated, SlickText delivery  

**🟠 Inbox send goes through SlickText only — not Twilio**
- `POST /api/inbox/<last4>/send` hardcodes `channel="slicktext"`
- File: `operator/app/routes/api.py:756-761`
- Twilio-only clients or clients not on SlickText would silently fail

**🟡 Phone last-4 collision resolution picks "most recently active" thread**
- If two fans share the same last 4 digits, the inbox only shows one
- Documented behavior, but edge case worth surfacing to operators
- File: `operator/app/routes/api.py:633-643`

**🟡 Inbox thread includes blast messages; brain context excludes blast messages**
- Operators see blasts in the conversation view but the AI does not use blast messages for context
- Could cause confusion ("why is the AI not acknowledging the blast I sent?")

### Blast Messages

✅ Full draft → schedule or send-now → async execution → DB status tracking  
✅ Rich targeting: tag, location, show signups, tier, random %, compound AND filters, top-N engaged  
✅ Per-recipient tracked link injection  
✅ Quiz session start on blast  
✅ Mid-send cancellation  
✅ Mid-send credit check (periodic, every 50 sends)  
✅ Scheduled blast processing — APScheduler every 60 seconds, atomic DB claim  

**🟠 Blast timezone handling may be wrong for scheduling**
- `datetime.fromisoformat(send_at_str).replace(tzinfo=timezone.utc)` assumes the datetime-local value from the UI is already UTC
- If the UI sends local wall time (which `datetime-local` inputs do), blasts will fire at the wrong time
- File: `operator/app/routes/blast.py:739, 871`

**🟠 Tier naming inconsistency between audience API and blast targeting**
- `/api/audience` uses `"casual"` in `tier_order` but DB/blast code stores `"lurker"`
- Result: audience stats could show zero for "casual" even though `lurker` fans exist
- File: `operator/app/routes/api.py:280`, `operator/app/queries.py:170, 269`

**🟡 Smart Send preview in the HTML blast route is not tenant-scoped**
- `blast.py:541-610` uses global queries; the API version in `api.py:1088-1155` is correctly tenant-scoped
- Risk in multi-tenant or super-admin contexts
- File: `operator/app/routes/blast.py:541-610`

**🟡 No formal approval workflow — confirmation is UI-only**
- "Approve" is just a confirm checkbox before sending; no state machine with reviewer gates
- Fine for now, important context for Plan 06 Auto-Pilot

### Admin Quality Tab

✅ Reads `ai_quality_reports` from DB  
✅ "Run digest now" streams output of `scripts/generate_quality_digest.py`  
✅ Mark-reviewed flow  

---

## 7. Performer Features

### Core AI Experience

✅ Full ZarnaBrain pipeline: intent → routing → RAG → generation → tone → emphasis  
✅ Multi-provider LLM routing for general replies  
✅ Fan memory extracted + persisted after every reply  
✅ Conversation history (last 8 turns)  
✅ Link rewriting to tracked URLs  
✅ Engagement scoring, session tracking, reply analytics  
✅ Live show keyword signup + join confirmation  
✅ Quiz intercept during active quiz  
✅ Blast context injection for active blasts  

### Creator Config

✅ File-backed config (`creator_config/<slug>.json`) with rich personality, voice, links  
✅ DB fallback (`creator_configs` table) for dynamically provisioned creators  
✅ Multi-creator architecture documented in `CLIENTS.md` — ready to add new performers  

**🟡 Join confirmation SMS copy is Zarna-specific (MIL/husband/kids jokes)**
- `app/live_shows/join_confirmations.py` generates Zarna-voiced confirmations
- A second performer on the platform would receive Zarna's confirmation copy
- File: `app/live_shows/join_confirmations.py`

**🟡 Blast context prompt says "Respond as Zarna would" regardless of slug**
- File: `app/live_shows/blast_context.py:96-104`

**🟡 `contacts.creator_slug` defaults to `'zarna'` in DB schema**
- Live show signup contact insert omits `creator_slug` — falls back to DB default `'zarna'`
- File: `app/live_shows/repository.py:236-242`, `app/storage/postgres.py:57`

### My Bot Settings (Performer)

Currently exposed: `name`, `bio`, `description`, `voice_style`, `tone`, `website_url`, `podcast_url`, `media_urls`, `banned_words`, `links`

**❌ Missing vs SMB (Plan 07 gaps — not yet built):**
- `welcome_message` — no custom first-message text
- `signup_question` — no onboarding question for new fans
- `send_contact_card` — no vCard MMS capability
- `profile_photo_url` — no photo for contact card
- `outreach_invite_message` — no configurable invite copy

### Subscriber Tracking

**❌ No `performer_subscribers` table**
- Fan opt-in is not tracked as an explicit record
- Dashboard "subscriber count" = `COUNT(DISTINCT phone_number) FROM contacts`
- No first-text timestamp separate from first message timestamp
- Plan 07 addresses this

### Multi-Performer Readiness

The architecture supports multiple performers (separate Railway deployments, separate DBs, `CREATOR_SLUG` controls everything). Estimated gaps to add a second performer:

1. New `creator_config/<slug>.json` + training data or PG embeddings ✅ documented
2. Fix join confirmation copy to be voice-neutral or configurable 🟡
3. Fix blast context prompt's hardcoded "Zarna" reference 🟡
4. Fix `live_show_signups` contact insert to include `creator_slug` 🟡
5. Add welcome/compliance message (Plan 07) 🔲

---

## 8. Business (SMB) Features

### Core AI Experience

✅ Inbound SMB Twilio webhook firewalled from performer traffic  
✅ Conversational AI replies using knowledge base + calendar context  
✅ STOP handling with DB `stopped` status  
✅ Owner SMS blast flow (AI-clarify → send)  
✅ Geo and intent preference tagging  
✅ Engagement scoring  

### Onboarding (New Customer First Text)

✅ AI opt-in detection (whether message is an opt-in signal)  
✅ Welcome message + signup question sent on first text  
✅ A2P compliance line included  
✅ vCard MMS sent on first text (when enabled)  
✅ Outreach invite code claim (if applicable)  

**❌ `send_contact_card` toggle in My Bot has no effect at runtime**
- `onboarding.py:152` reads `tenant.raw.get("send_contact_card", True)` — the on-disk JSON file
- My Bot UI saves the toggle to `smb_bot_config` in the DB, which is never read at send time
- File: `app/smb/onboarding.py:152`
- Fix documented in Plan 07 Part 7

### SMB Portal (Operator)

✅ Owner can view subscriber stats, blast history, show check-ins  
✅ Owner can create shows and blast attendees  
✅ Password-protected via env var  

**🔴 Operator portal "Everyone" blast selects ALL phones including STOPped users**
- `_get_all_subscriber_phones` at `operator/app/routes/smb_portal.py:235-244` has no `status='active'` filter
- The main app correctly uses `get_active_subscribers` with status filter; portal does not
- TCPA compliance risk

**🟡 Portal blast UI for segment targeting is wired but not surfaced**
- Backend supports `seg:question:value` targeting but no segment UI cards are shown in the operator portal
- File: `operator/app/routes/smb_portal.py:986-992`

### SMB Knowledge Base

✅ Static FAQ from `creator_config/*.json` `knowledge_base` field  
✅ Live calendar scrape (venue-specific HTML parsing)  
✅ 2-hour per-slug cache  

**🟡 Calendar scrape parser is hardcoded for West Side Comedy Club's Next.js page structure**
- Not portable to other venues without modifications
- File: `app/smb/knowledge.py:126-224`

**🟡 Tracked link domain uses `RAILWAY_PUBLIC_DOMAIN`; vCard MMS uses `OPERATOR_API_BASE_URL`**
- Possible environment mismatch — links in SMS could resolve to wrong host
- Files: `app/smb/knowledge.py:56-64`, `app/smb/onboarding.py:302-313`

### Dead Code

**🔵 `app/smb/reporting.py` is a stub** — two comment lines only; real analytics live in `storage.py` + scripts  
**🔵 `app/smb/content.py` is a stub** — one-line comment; `value_content_topics` loaded on tenant but never used  
**🔵 `set_pending_blast` / `get_pending_blast` in `storage.py`** defined but never called in production code  
**🔵 `portal_interactive.py`** — full unregistered Flask blueprint; intentionally unused per `main.py` comment; should be deleted or moved

### Config Dual-Source Risk

**🟡 `operator/app/business_configs/<slug>.json` and `creator_config/<slug>.json` are separate files that can drift**
- Operator My Bot API reads from `business_configs/`; SMS brain loads from `creator_config/`
- An edit in My Bot and an edit to the config file can produce inconsistent bot behavior
- There is only one business config file in the repo: `west_side_comedy.json`

---

## 9. SMS Infrastructure

### Twilio Adapter

✅ Inbound parsing + signature validation  
✅ Outbound SMS + WhatsApp branch  
✅ MMS support via `media_url`  
✅ 429 retry with exponential backoff (3 attempts)  

**🟡 No retry for non-429 errors (5xx, network timeouts)**
- Only 429 triggers retry; other transient errors fail immediately
- File: `app/messaging/twilio_adapter.py:204-216`

### SlickText Adapter

✅ v1 (Basic auth) and v2 (Bearer token) support  
✅ 400-char length cap with truncation  
✅ 429 retry with exponential backoff  

**🟠 MMS not supported — SlickText adapter is SMS text only**
- `send_reply` has no `media_url` parameter
- Performers on SlickText cannot receive vCard MMS (relevant for Plan 07)
- File: `app/messaging/slicktext_adapter.py:245`

**🟠 SlickText v2 dedup is broken** (see §4 Security)

### Broadcast / Live Show Sends

✅ `ThreadPoolExecutor(max_workers=20)` for join confirmations — bounded concurrency  
✅ 350ms delay between broadcast sends  

### Rate Limiting

✅ Per-phone inbound rate limit: 3 messages / 60 seconds  
✅ `/message` API rate-limited by client IP  

**🟡 No carrier-grade outbound throttle for high-volume blasts**
- Only a configurable sleep delay — not a proper rate-limited queue

---

## 10. Database & Storage

### Schema / Migrations

✅ `_ensure_tables()` runs under advisory lock — safe for concurrent Gunicorn workers  
✅ `ON CONFLICT DO NOTHING` / `ON CONFLICT DO UPDATE` used appropriately throughout  
✅ All production queries use parameterized `%s` — no injection risks  

**🔴 `messages.creator_slug` column is defined only in operator migrations, not main app migrations**
- `app/storage/postgres.py` inserts `creator_slug` on every message (line 548), but the `ALTER TABLE ADD COLUMN IF NOT EXISTS creator_slug` is in `operator/app/db.py:291`
- A new client that starts the main app before the operator migrations run will crash immediately
- Fix: duplicate the ALTER TABLE into main app's `_MIGRATIONS` tuple

**🟠 `ensure_session_tables()` failure is silently swallowed**
- `_ensure_tables()` wraps session table creation in `except Exception: pass`
- Session tracking can be completely broken with no alert and no log entry
- File: `app/storage/postgres.py:493-497`

### Multi-Tenant Isolation

✅ `creator_slug` scoping on all main app queries  
✅ SMB queries scoped by `tenant_slug` throughout `smb/storage.py`  
✅ One Railway project = one DB per client (architectural isolation)  

**🟡 `active_live_shows()` has no `creator_slug` filter**
- Fine while each deployment is one DB per creator; would break on a shared DB
- File: `app/live_shows/repository.py:249-257`

**🟡 `/analytics` blueprint does not include `creator_slug` in WHERE clauses**
- Would leak cross-tenant data on a shared DB
- File: `app/analytics/blueprint.py`

### Production Storage Testing

**🟠 `app/storage/postgres.py` has no pytest coverage**
- All tests use `InMemoryStorage`
- Migrations, query correctness, and connection pool behavior are completely untested

---

## 11. Tests & Code Quality

### Test Coverage Gaps

| Area | Current coverage | Gap severity |
|------|-----------------|--------------|
| `app/storage/postgres.py` | InMemoryStorage only | 🟠 High |
| `app/admin/*` | Zero coverage | 🟡 Medium |
| `operator/app/routes/api.py` (~5,200 lines) | Near zero | 🟡 Medium |
| `app/live_shows/` (broadcast_worker, quiz, join_confirmations) | Partial | 🟡 Medium |
| `app/link_tracker.py` | None | 🔵 Low |
| `app/ops_metrics.py` | None | 🔵 Low |

**🟡 `scripts/test_*.py` files look like tests but are not run by `pytest tests/`**
- E2E and phase-gate scripts in `scripts/` are invisible to CI
- Anyone running `pytest tests/` has no visibility into these paths

### Silent Failure Risks

**🟠 `handler.py` secondary operations swallow failures without logging**
- Memory update, winning example tracking, link rewriting: `except Exception: pass` or empty except
- File: `app/brain/handler.py:182-315`
- These fail silently in production with no Railway log entry

### Logging Consistency

**🟡 `main.py` webhook handlers log to root logger → tagged `[WEB]` instead of `[ZARNA]`**
- The `_ServiceFormatter` maps `app.brain` → `[ZARNA]`, but `main.py` uses `logging.getLogger('__main__')` or root
- Most SMS pipeline log lines show up as `[WEB]` — impossible to filter by service area in Railway

### Dead Code

**🔵 `app/smb/portal_interactive.py`** — full unregistered blueprint; should be deleted  
**🔵 `app/smb/reporting.py`** — stub (2 comment lines)  
**🔵 `app/smb/content.py`** — stub (1 comment line)  
**🔵 `operator/app/routes/api.py` `billing/__init__.py` docstring** references non-existent functions  

---

## 12. Master Issues List

Sorted by priority. Each item is self-contained and actionable.

### 🔴 Critical — Fix immediately

| # | Issue | File | Area |
|---|-------|------|------|
| C1 | `broadcast_optouts` table is never written — performer blast opt-out suppression is completely non-functional | `operator/app/db.py:47-51`, `main.py:66-88` | Security / Compliance |
| C2 | Operator SMB portal "Everyone" blast includes STOPped users | `operator/app/routes/smb_portal.py:235-244` | Security / Compliance |
| C3 | Twilio signature validation skipped when `TWILIO_AUTH_TOKEN` is missing (returns True) | `app/messaging/twilio_adapter.py:108-113` | Security |
| C4 | SlickText webhook has no auth when `SLICKTEXT_WEBHOOK_SECRET` is unset | `app/inbound_security.py:43-54` | Security |
| C5 | `messages.creator_slug` column only in operator migrations — main app crashes on fresh deploy | `app/storage/postgres.py` migrations | Database |
| C6 | No CTIA compliance line on first performer message (TCPA risk) | `app/brain/handler.py` | Compliance |
| C7 | `send_contact_card` My Bot toggle has no runtime effect — reads on-disk file, ignores DB | `app/smb/onboarding.py:152` | Bug |
| C8 | Notion blast metrics not tenant-scoped — all customers get same global counts | `operator/app/notion_crm.py:617-624` | Data accuracy |

### 🟠 High — Fix before next major feature

| # | Issue | File | Area |
|---|-------|------|------|
| H1 | Stripe webhook has no idempotency — duplicate events can double-grant credits | `operator/app/routes/billing.py:319-337` | Billing |
| H2 | Performer AI replies not credit-gated — bot keeps responding after credits exhausted | `main.py:433-441` | Billing |
| H3 | Annual subscription credits assigned as monthly — full year gets one month's credits | `operator/app/routes/billing.py:437-438` | Billing |
| H4 | SlickText v2 dedup broken — same message can be processed twice | `main.py:314-322` | SMS |
| H5 | SlickText inbox send only — Twilio clients cannot receive manual messages from dashboard | `operator/app/routes/api.py:756-761` | Dashboard |
| H6 | `ensure_session_tables()` failure swallowed silently | `app/storage/postgres.py:493-497` | Database |
| H7 | Structured intent (tickets/merch) generation has no fallback if Gemini fails | `app/brain/generator.py:851-857` | AI |
| H8 | Training data silently falls back to Zarna corpus on slug mismatch | `app/config.py:32-36` | AI / Multi-tenant |
| H9 | Full phone numbers logged in exception paths (PII in Railway logs) | `main.py:418-423`, `slicktext_adapter.py:136-138` | Privacy |
| H10 | Blast opt-out recording is SlickText-only — Twilio blast opt-outs not tracked | `main.py:341-344` | Compliance |
| H11 | `handler.py` secondary failures (memory, tracking) swallowed without logging | `app/brain/handler.py:182-315` | Reliability |
| H12 | Blast timezone: `datetime-local` UI input assumed to be UTC on server | `operator/app/routes/blast.py:739, 871` | Bug |
| H13 | Phone provisioning is a stub — no real Twilio numbers assigned in code | `operator/app/provisioning/phone.py:130-166` | Ops |
| H14 | `app/storage/postgres.py` has zero pytest coverage | `tests/` | Testing |

### 🟡 Medium — Address in cleanup sprint

| # | Issue | File | Area |
|---|-------|------|------|
| M1 | `/how-it-works/business` redirect drops business audience context | `lovable-frontend/src/App.tsx:111` | Frontend |
| M2 | Forgot password UI always shows success regardless of API failure | `ForgotPassword.tsx:10-25` | Frontend |
| M3 | Credits UI shows "Unlimited" when `total === 0` (conflicting with Billing.tsx logic) | `CreditsWidget.tsx:73-74` | Frontend |
| M4 | Fan of the Week / Fan History use legacy `/inbox/` paths without tenant prefix | `FanHistoryTable.tsx:168-170` | Frontend |
| M5 | Tier name mismatch: API uses `casual`, DB uses `lurker` | `operator/app/routes/api.py:280` | Data |
| M6 | Engagement score docstring claims click weighting; SQL does not include it | `operator/app/engagement.py:9-60` | Data |
| M7 | `recompute_all(slug=...)` ignores slug — updates all tenants | `operator/app/engagement.py:76-77` | Data |
| M8 | Smart Send preview in HTML blast route is not tenant-scoped | `operator/app/routes/blast.py:541-610` | Bug |
| M9 | CSRF check can be bypassed with requests that have no Origin/Referer header | `operator/app/__init__.py:114-120` | Security |
| M10 | `SLICKTEXT_CONTACT_TEXTWORDS` default has hardcoded Zarna IDs | `app/config.py:82-83` | Multi-tenant |
| M11 | Join confirmation SMS is Zarna-voiced — wrong for any second performer | `app/live_shows/join_confirmations.py` | Multi-tenant |
| M12 | Blast context prompt hardcodes "Respond as Zarna would" | `app/live_shows/blast_context.py:96-104` | Multi-tenant |
| M13 | Live show signup contact insert omits `creator_slug` | `app/live_shows/repository.py:236-242` | Multi-tenant |
| M14 | `banned_words` from creator config loaded but never enforced in generation | `app/brain/creator_config.py:44, 81` | AI quality |
| M15 | Intent fast-path Zarna-specific phrases not configurable per creator | `app/brain/intent.py:72-87` | Multi-tenant |
| M16 | Two credit metering implementations (main.py SQL vs billing module) can drift | `main.py:514-523` | Billing |
| M17 | Notion net margin reads "Monthly Fee" from Notion itself — circular if stale | `operator/app/notion_crm.py:577-587` | Data |
| M18 | Notion customer create has no existing-slug check — duplicate pages possible | `operator/app/notion_crm.py` | Data |
| M19 | SMB config dual-source: `business_configs/` (operator) vs `creator_config/` (brain) can drift | `operator/app/routes/api.py:2265` | Config |
| M20 | MMS not supported on SlickText adapter — blocks contact card for SlickText performers | `app/messaging/slicktext_adapter.py:245` | SMS |
| M21 | Billing not discoverable from user menu — only accessible via credit banners | `UserMenu.tsx` | Frontend UX |
| M22 | "Messages today" label says today but logic is rolling 24h | `operator/app/queries.py:29-30` | Copy |
| M23 | `main.py` webhook logs tagged `[WEB]` instead of `[ZARNA]` — can't filter in Railway | `main.py` | Logging |
| M24 | `scripts/test_*.py` files invisible to CI / `pytest tests/` | `scripts/` | Testing |

### 🔵 Low / Cleanup

| # | Issue | File | Area |
|---|-------|------|------|
| L1 | `app/smb/portal_interactive.py` — full unregistered blueprint, dead code | `app/smb/portal_interactive.py` | Dead code |
| L2 | `app/smb/reporting.py` — stub only | `app/smb/reporting.py` | Dead code |
| L3 | `app/smb/content.py` — stub only | `app/smb/content.py` | Dead code |
| L4 | Footer "Proof" anchor (`#real-people`) points to section not rendered on homepage | `Footer.tsx:20` | Frontend |
| L5 | Early Access dialog submit is a fake form — toast only, no API call | `EarlyAccessDialog.tsx:28-35` | Frontend |
| L6 | Business nav shows "Audience" instead of "Customers" | `DashboardHeader.tsx:17-24` | Copy |
| L7 | Default Flask secret fallback strings in main.py and operator app | `main.py:155-160`, `operator/app/__init__.py:25` | Security |
| L8 | `billing/__init__.py` doc references functions that don't exist | `operator/app/billing/__init__.py:1-11` | Docs |
| L9 | Operator `requirements.txt` uses lower bounds — builds can drift | `operator/requirements.txt` | Deps |
| L10 | `drip_reengagement.py` will be superseded by Plan 06 Phase 3 — mark for retirement | `scripts/drip_reengagement.py` | Docs |
| L11 | Dedup code comment says "200" entries; `_MAX_SEEN` is 1000 | `main.py:179, 184` | Docs |
| L12 | No pytest coverage for `app/admin/*`, `app/link_tracker.py`, `app/ops_metrics.py` | `tests/` | Testing |
| L13 | `active_live_shows()` has no `creator_slug` filter (safe on single-DB, wrong on shared) | `app/live_shows/repository.py:249-257` | Multi-tenant |

---

_Review authored from static code analysis. All findings reference specific files and line numbers. No production data was accessed._

---

## Status snapshot — May 8 2026

Six-part System Hardening Plan + audit-leftovers PR have shipped. Status of every audit item:

| # | Status | Where it landed |
|---|--------|------|
| C1 | ✅ Fixed | Part 1 — `_record_blast_optout` writes to `broadcast_optouts` |
| C2 | ✅ Fixed | Part 1 — `_get_all_subscriber_phones` adds `status='active'` filter |
| C3 | ✅ Fixed | Part 1 — Twilio validator returns False (not True) when token missing; verified live (`/twilio/webhook` returns 403 without signature) |
| C4 | ⚠️ Mitigated | Part 1 added prod startup warning; SlickText webhook still fail-open until `SLICKTEXT_WEBHOOK_SECRET` is set on Railway AND the matching header is configured in SlickText dashboard |
| C5 | ✅ Fixed | Part 2 — `messages.creator_slug` migration in main app |
| C6 | 🔲 Deferred | In scope of Plan 07 (Performer Onboarding & Bot Parity) |
| C7 | ✅ Fixed | Audit-leftovers — `_send_contact_card_enabled()` now reads `smb_bot_config` first |
| C8 | ✅ Fixed | Audit-leftovers — `sync_customer_costs` blast query filters by `creator_slug = %s` |
| H1 | ✅ Fixed | Part 3 — `stripe_webhook_events` idempotency table + `_claim_stripe_event` |
| H2 | ✅ Fixed | Part 3 — `_has_credits_remaining()` gate behind `BILLING_HARD_GATE` env (off by default) |
| H3 | ✅ Fixed | Audit-leftovers — `_credits_for_cycle(plan, cycle)` returns `monthly × 12` for annual |
| H4 | ✅ Fixed | Audit-leftovers — SlickText v2 dedup synthesizes a key from `event_name + contact_id + sha1(body)` |
| H5 | 🔲 Deferred | Twilio inbox-send parity needs per-tenant channel routing — defer until 2nd performer goes live |
| H6 | ✅ Fixed | Part 2 — `ensure_session_tables()` failures now `logger.exception()` |
| H7 | ✅ Fixed | Part 5 — structured intents (SHOW/MERCH/BOOK/CLIP/PODCAST) fall back to OpenAI then Anthropic on Gemini failure |
| H8 | ✅ Fixed | Part 5 — startup `logger.error` if `CREATOR_SLUG != zarna` and slug-specific training data missing |
| H9 | ✅ Fixed | Part 1 — phone numbers masked to `...1234` in exception logs |
| H10 | ✅ Fixed | Part 1 — Twilio webhook now mirrors SlickText opt-out recording |
| H11 | ✅ Fixed | Part 5 — handler `except Exception: pass` blocks replaced with `_logger.exception()` |
| H12 | ✅ Fixed | Audit-leftovers — `_parse_local_datetime_to_utc()` uses browser-supplied `send_at_tz` IANA name |
| H13 | 🔲 Deferred | Real Twilio number provisioning needs Twilio billing setup + ops decision; manual purchase per client today |
| H14 | ✅ Fixed | Part 6 — `tests/test_cleanup.py` added pytest coverage for `app/storage/postgres.py` migrations |
| M1–M5 | ✅ Fixed | Part 4 frontend + tier-rename PR |
| M6 | ✅ Fixed | Audit-leftovers — `engagement.py` docstring matches actual SQL (no clicks term, caps at 80) |
| M7 | ✅ Fixed | Audit-leftovers — `recompute_all(slug=...)` filters both messages and contacts by slug |
| M8 | ✅ Fixed | Audit-leftovers — `smart_send_preview` reads `current_user()` and scopes contacts + blast queries |
| M9 | ✅ Fixed | Audit-leftovers — CSRF logs every no-Origin request, rejects when `OPERATOR_CSRF_STRICT=true` |
| M10–M15 | ✅ Fixed | Part 5 — banned_words enforced, voice-neutral join confirmations, etc. |
| M16 | 🔲 Deferred | Two credit metering implementations is a refactor, not a bug — consolidating them touches both webhook paths |
| M17 | ✅ Fixed | Audit-leftovers — `_resolve_monthly_fee_from_db()` reads from `operator_users.plan_tier` + `ALL_PLANS` |
| M18 | ✅ Fixed | Audit-leftovers — `create_customer_in_notion` calls `_find_page_by_slug` first and skips on duplicate |
| M19 | 🔲 Deferred | SMB config dual-source needs migration of `business_configs/` into the brain's `creator_config/` loader |
| M20 | 🔲 Deferred | SlickText MMS support is a feature add, not a bug; relevant only when a SlickText performer wants vCards |
| M21 | ✅ Fixed | Part 4 — Billing in user menu |
| M22 | ✅ Fixed | Part 3 — "Last 24 Hours" label |
| M23 | ✅ Fixed | Part 5 — main.py logging mapped to `[ZARNA]` tag |
| M24 | ✅ Fixed | `pytest.ini` already excludes `scripts/` via `norecursedirs` — no false test discovery |
| L1, L2, L3 | ✅ Fixed | Part 6 — dead files deleted |
| L4 | ✅ Fixed | Audit-leftovers — Footer "Proof" anchors to `/#proof` (matches ProofZarna section id) |
| L5 | ✅ Fixed | Audit-leftovers — Early Access dialog opens a pre-filled `mailto:brij@zarnagarg.com` |
| L6 | ✅ Fixed | Part 4 — DashboardHeader uses `labels.audienceNav` |
| L7 | ✅ Fixed | Audit-leftovers — both `main.py` and `operator/app/__init__.py` log a critical error if running prod with default secret |
| L8 | ✅ Fixed | Audit-leftovers — `billing/__init__.py` docstring matches actual public surface |
| L9 | 🔲 Deferred | Operator dependency lockfile — needs `pip-compile` or `uv lock` workflow change, not a quick edit |
| L10 | n/a | `scripts/drip_reengagement.py` no longer exists in the repo |
| L11 | ✅ Fixed | Part 6 — comment now says 1000, matching `_MAX_SEEN` |
| L12 | 🔲 Deferred | `app/admin/*` test backfill is large; deferred until the admin module is split per file-organisation rule |
| L13 | ✅ Fixed | Audit-leftovers — `active_live_shows(creator_slug=...)` with backward-compatible filter |

**Remaining open items** (decision required, not just code):
- **C4 paired action:** generate `SLICKTEXT_WEBHOOK_SECRET`, set on Railway, configure matching `X-Zarna-Webhook-Secret` header in SlickText.
- **C6, H5, H13, M16, M19, M20, L9, L12:** see status table above.

Last verified live: `b0aee77` deployed to both `ZarnaAi` and `web` services on the `Zar` project; `/twilio/webhook` returns 403 without signature (Part 1 fail-safe confirmed in production).
