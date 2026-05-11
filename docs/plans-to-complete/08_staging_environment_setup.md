# Staging Environment Setup Plan

**Status:** FULLY LIVE end-to-end as of 2026-05-10. Both backend services deploy on every push to the `staging` branch. The Lovable staging frontend (`zar-chat-magic.lovable.app`, a remix of the prod `zar-fan-connect`) is wired to the staging operator with CORS verified. You can sign up, log in, view the dashboard, send blasts, hit credit limits, and run Stripe test checkouts — all in your browser, nothing touches prod.  
**Goal:** A fully isolated staging environment that mirrors prod so we can test blasts, credit limits, billing plans, and Stripe flows without touching real fans or charging real money.

---

## Quick reference — live staging URLs and credentials

| Thing | Value |
|---|---|
| **Main app (SMS)** | https://web-production-d7b70.up.railway.app |
| **Operator (API + dashboard backend)** | https://operator-production-9330.up.railway.app |
| **Lovable frontend (staging dashboard)** | https://zar-chat-magic.lovable.app — remix of prod `zar-fan-connect`, hardcoded API base + Stripe test key + Google Client ID, orange "STAGING" banner pinned to the top of every page |
| **Staging Twilio number** | `+1 (573) 229-0656` |
| **Operator login email** | `brijgarg286@gmail.com` |
| **Operator login password** | (ask Brij — stored in 1Password as "Zarna Staging — operator login") |
| **Trial credits seeded** | `10` (so credit limit hits fast) |
| **Stripe test card** | `4242 4242 4242 4242`, any future expiry, any CVC, any ZIP |
| **Stripe webhook ID** | `we_1TVLc9HCxNGsWyPBXmx3NavI` → `https://operator-production-9330.up.railway.app/api/billing/webhook` |
| **Twilio webhook** | SMS → `https://web-production-d7b70.up.railway.app/twilio/webhook` |
| **API_SECRET_KEY** (for direct `/message` tests, no Twilio sig needed) | (ask Brij — stored in 1Password as "Zarna Staging — API_SECRET_KEY") |

Pre-seeded test fans (creator_slug = `brij-test`):
| Phone | Notes |
|---|---|
| `+15005550006` | Twilio "magic" number — accepts SMS (no real delivery, but the send succeeds) |
| `+15005550008` | Twilio magic — invalid number (use to test failure paths) |
| `+15005550010` | Twilio magic — unavailable (use to test retry/queue) |
| `+15005550003` | Twilio magic — international forbidden |

> Twilio magic numbers don't deliver real SMS but the API call returns success/failure as if it did. To test a real-phone delivery, add your own number as a fan via the operator dashboard, then send a blast.

### One-line smoke tests

```bash
# Main app health
curl https://web-production-d7b70.up.railway.app/health
# → {"service":"zarna-ai","status":"ok"}

# Operator health
curl https://operator-production-9330.up.railway.app/health
# → 200 OK

# Direct message-the-bot test (no Twilio needed) — verified working end-to-end.
# Get API_SECRET_KEY from Brij (1Password → "Zarna Staging — API_SECRET_KEY").
curl -X POST https://web-production-d7b70.up.railway.app/message \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $STAGING_API_SECRET_KEY" \
  -d '{"phone_number": "+15005550006", "message": "hi bot, who are you?"}'
# → {"reply": "Hey there! I'm a test bot for Brij's staging environment...", "skipped": false}
```

> **Re-running the seed** is idempotent — every insert uses ON CONFLICT DO UPDATE. Re-run any time with the command in [Step 9](#step-9--seed-script-completed) below.

---

## Architecture

Three deployments mirror the three prod surfaces:

| Staging Deployment | Mirrors | Hosting | Purpose |
|---|---|---|---|
| `zarna-staging` | main Flask SMS app | Railway | Inbound SMS, AI replies, blast receives |
| `zarna-operator-staging` | operator service (API + dashboard backend) | Railway | Credit gates, plan upgrades, blast sends, Stripe webhooks |
| `zar-fan-connect-staging` | React frontend (lives outside this repo, hosted on Lovable) | Lovable preview/branch | The dashboard UI Veer actually opens in his browser |

Both use:
- A **Twilio staging subaccount** with a dedicated test number (real SMS to your phone only)
- **Stripe test mode** (`sk_test_...`) — no real charges ever
- **Isolated Postgres DBs** — zero overlap with prod fan data
- `CREATOR_SLUG=brij-test` — separate creator config, separate training data

### Branch strategy

- **Prod** (`zarna-prod` + `zarna-operator-prod`): auto-deploys from `main` only
- **Staging** (`zarna-staging` + `zarna-operator-staging`): auto-deploys from a dedicated `staging` branch

This means: open a PR to `staging`, deploys to staging automatically, test there, then merge `staging` → `main` for prod. Without this separation you can't "verify on staging before going to prod" because both would deploy at the same time.

### Known limitations

- **SlickText flows can't be tested on staging.** Prod ingests some inbound via SlickText; staging only uses Twilio. Any feature that depends on a SlickText webhook (e.g. SMB inbound for WSCC) is prod-only.
- **No real fan replies.** The staging AI runs on real Gemini, but it only replies to people you've added to the staging DB as fans.

---

## Resources (live)

| Resource | Details |
|---|---|
| Twilio staging subaccount | Name: "Zarna Staging" — SID + auth token in Brij's 1Password and on both Railway services as `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` |
| Staging phone number | `+1 (573) 229-0656` — SID stored alongside the subaccount creds |
| Railway `zarna-staging` project | ID: `010345f4-4e61-48d1-bfcc-4b256cfa49f7`, Env ID: `4daaf956-b9b0-49fe-a799-1c9e6796a244` |
| Railway `zarna-staging` web service | ID: `0d3be54b-3b8c-433f-9224-e51f52dca732`, root: `/`, branch: `staging` |
| Railway `zarna-staging` Postgres | ID: `4481f2f8-08e5-4c81-9dc6-2a6be5287ec7` (provisioned via `templateDeployV2` — failed image-only attempt was 0e5cdcd7) |
| Railway `zarna-operator-staging` project | ID: `ee8ee891-7ace-44d0-8582-d6b0078d263e`, Env ID: `41f7beda-dea2-445c-9d13-86383f6f7caf` |
| Railway `zarna-operator-staging` operator service | ID: `0b929cca-565b-4ec4-ab97-57cee3094d4c`, root: `/operator`, branch: `staging` |
| Railway `zarna-operator-staging` Postgres | ID: `2f5fa17f-c929-4f12-8387-df4217e443d1` |
| Stripe webhook | ID: `we_1TVLc9HCxNGsWyPBXmx3NavI`, secret stored on both Railway services as `STRIPE_WEBHOOK_SECRET` |

> All Twilio creds, Stripe keys, and Railway IDs are also stored in `.env` and on the Railway services themselves — single source of truth is Railway.

---

## What was done (history of the actual setup, May 2026)

For posterity / future debugging — the actual sequence that was executed:

1. ✅ Created `staging` git branch off `origin/main` and pushed.
2. ✅ Created `creator_config/brij-test.json` + empty training data stubs and committed to `staging`.
3. ✅ Verified existing Stripe Price IDs in `.env` are already test mode (`livemode=false`) — reused them, no new Stripe products needed.
4. ✅ Provisioned Postgres on both Railway projects via `templateDeployV2` (initial `serviceCreate` with just an image failed — `templateDeployV2` is the correct API).
5. ✅ Pulled `DATABASE_URL` (internal) and `DATABASE_PUBLIC_URL` (viaduct) from each Postgres.
6. ✅ Created `web` (main app) + `operator` Railway services pointing at the GitHub repo + `staging` branch via `serviceCreate { source: { repo } branch: "staging" }`.
7. ✅ Set `rootDirectory: /operator` on the operator service via `serviceInstanceUpdate` (without this, both services build the main app, not the operator).
8. ✅ Set ~50 env vars on each service via `variableCollectionUpsert`.
9. ✅ Generated public domains via `serviceDomainCreate`, then explicitly set `targetPort: 8080` via `serviceDomainUpdate` (Railway's auto port detection didn't pick up gunicorn's port).
10. ✅ Registered Stripe webhook → captured `whsec_...` → wrote it back to `STRIPE_WEBHOOK_SECRET` on both services → triggered redeploy.
11. ✅ Configured Twilio webhook on `+15732290656` to `https://web-production-d7b70.up.railway.app/twilio/webhook` (NOT `/sms` — that's a common mistake).
12. ✅ Wrote + ran `scripts/seed_staging_db.py` to seed operator user + 4 test fans.
13. ✅ Added `API_SECRET_KEY` so direct `POST /message` testing works without Twilio sigs.
14. ✅ Remixed the prod `zar-fan-connect` Lovable project into a new "Zar Staging" project (`zar-chat-magic.lovable.app`). Hardcoded API base, Stripe test publishable key, Google Client ID. Added the orange "STAGING — test environment" banner pinned to the top of every page.
15. ✅ Set `FRONTEND_URL` + Stripe checkout redirect URLs + `CORS_ALLOWED_ORIGINS` on the operator to point at the Lovable staging URL. CORS preflight verified.

---

## Lessons learned (read this before debugging Railway deploys)

| Symptom | Root cause | Fix |
|---|---|---|
| Postgres deploy `FAILED` immediately | Created service with just `image:` instead of using the official template | Use `templateDeployV2` with `template(code: "postgres")` config |
| Both services serve the main app even though one is "operator" | Railway defaults to repo root for builds | Set `rootDirectory: /operator` on the operator service via `serviceInstanceUpdate` |
| All routes return 404 from `railway-edge` server | `targetPort: null` — Railway proxy doesn't know which port to forward to | `serviceDomainUpdate` with `targetPort: 8080` (matches gunicorn bind port) |
| Stripe webhook returns 400 | Webhook URL was `/api/stripe/webhook` but the actual route is `/api/billing/webhook` | Update webhook endpoint URL via Stripe API |
| Twilio webhook gives no reply | URL was `/sms` but the route is `/twilio/webhook` | Update SmsUrl via Twilio API |

---

## Detailed step reference (for re-running parts of the setup)

### Step 1 — Create test creator config + training data stubs (COMPLETED)
Create in the repo:
- `creator_config/brij-test.json` — copy `zarna.json`, rename, edit persona to be obviously "test" so test replies are recognizable
- `training_data/brij-test_chunks.json` — empty array `[]`
- `training_data/brij-test_embeddings.json.gz` — empty gzipped array

Commit to the `staging` branch. Without these, the app crashes on first boot looking for `brij-test` files.

### Step 2 — Create Stripe test Price IDs
Use `STRIPE_SECRET_KEY=sk_test_...` from `.env` to call Stripe API and create test-mode Price IDs.

Plans (monthly + annual for each):
- `starter` — $79/mo, $790/yr, 3,200 credits
- `growth` — $149/mo, $1,490/yr, 6,200 credits
- `pro` — $299/mo, $2,990/yr, 12,500 credits
- `scale` — $599/mo, $5,990/yr, 25,200 credits
- `elite` — $999/mo, $9,990/yr, 41,900 credits
- `creator` — $1,999/mo, $19,990/yr, 80,300 credits
- `essentials` — $49/mo, $490/yr, 1,900 credits
- `standard` — $99/mo, $990/yr, 4,300 credits
- `business_pro` — $199/mo, $1,990/yr, 9,000 credits

Boosters (one-time):
- `mini` — $12, 500 credits
- `blast` — $32, 1,500 credits
- `big_send` — $79, 4,000 credits
- `power` — $179, 10,000 credits

### Step 3 — Pull staging DATABASE_URLs from Railway
Query Railway GraphQL API for `DATABASE_URL` from each Postgres service using the project/environment IDs above.

### Step 4 — Set env vars on both Railway staging projects
Using Railway GraphQL `variableCollectionUpsert`. Critical: set `STRIPE_WEBHOOK_SECRET=PLACEHOLDER_WILL_BE_SET_IN_STEP_6` for now — webhook signature verification will fail until Step 6, but the app will boot.

**Both projects** (real values are stored in Railway service vars + Brij's 1Password — NEVER paste them into this doc, GitHub Push Protection will reject the commit):
```
TWILIO_ACCOUNT_SID=<staging Twilio subaccount SID>   # from 1Password
TWILIO_AUTH_TOKEN=<staging Twilio auth token>        # from 1Password
TWILIO_PHONE_NUMBER=+15732290656
STRIPE_SECRET_KEY=sk_test_...                        # from .env
STRIPE_PUBLISHABLE_KEY=pk_test_...                   # from .env
STRIPE_WEBHOOK_SECRET=<placeholder, set in Step 6>
STRIPE_PRICE_ID_*=price_test_...                     # from Step 2
DATABASE_URL=<staging postgres url>                  # from Step 3
CREATOR_SLUG=brij-test
ENVIRONMENT=staging                                  # so logs are tagged + UI can show "STAGING" banner
```

**Operator-only (Stripe checkout redirect URLs):**
```
FRONTEND_URL=<staging Lovable URL>                   # set after Step 13
STRIPE_CHECKOUT_SUCCESS_URL=<frontend URL>/billing?status=success
STRIPE_CHECKOUT_CANCEL_URL=<frontend URL>/billing?status=cancel
CORS_ALLOWED_ORIGINS=<staging Lovable URL regex>
```

**Shared with prod (safe to copy same values):**
```
GEMINI_API_KEY
NOTION_TOKEN
RESEND_API_KEY
RESEND_FROM
```

**Main app only:**
```
SLICKTEXT_* vars     # optional — staging uses Twilio only
SMB_* vars
```

### Step 5 — Capture Railway URLs after first deploy
After Railway auto-deploys, query the Railway API for the public domain of each service:
- Main app: `https://zarna-staging-production.up.railway.app` (Railway-generated)
- Operator: `https://zarna-operator-staging-production.up.railway.app` (Railway-generated)

### Step 6 — Register Stripe webhook + update env var
Call Stripe API to register `<operator URL>/api/stripe/webhook` as a webhook endpoint. Subscribe to:
- `customer.subscription.created`
- `customer.subscription.updated`
- `customer.subscription.deleted`
- `checkout.session.completed`
- `invoice.payment_succeeded`
- `invoice.payment_failed`

Capture the returned `whsec_...` secret, write it back to `STRIPE_WEBHOOK_SECRET` on the operator project, trigger a redeploy.

### Step 7 — Configure Twilio webhook on staging number
Call Twilio API to set the SMS webhook URL on `+15732290656`:
- `SmsUrl`: `https://zarna-staging-production.up.railway.app/sms`
- `SmsMethod`: `POST`
- `StatusCallback`: `https://zarna-staging-production.up.railway.app/sms/status`

Without this, inbound SMS to the staging number goes nowhere.

### Step 8 — Verify DB schema migrations
The app's `ensure_tables()` should run on first boot. Verify by:
- Reading staging app logs for "ensure_tables" success messages
- Or calling staging operator's `/api/health` endpoint and confirming 200 OK
- If it didn't auto-run: connect to staging Postgres directly and execute migrations

### Step 9 — Seed script (COMPLETED)

`scripts/seed_staging_db.py` is in the repo. Re-run any time. Pull the public DB URLs from the Railway dashboard (Postgres service → Variables → `DATABASE_PUBLIC_URL`, NOT `DATABASE_URL` — the internal one only resolves inside Railway):

```bash
cd "/Users/brijgarg/Zarna Project"
pip install psycopg2-binary bcrypt   # one-time

# All three values come from 1Password ("Zarna Staging — *") or Railway:
STAGING_MAIN_DB_URL='<DATABASE_PUBLIC_URL from zarna-staging Postgres service>' \
STAGING_OPERATOR_DB_URL='<DATABASE_PUBLIC_URL from zarna-operator-staging Postgres service>' \
STAGING_OWNER_PASSWORD='<bootstrap password for the operator owner account>' \
python3 scripts/seed_staging_db.py
```

Brij keeps a `.env.staging` in 1Password with all three values filled in — `dotenv -f .env.staging run -- python3 scripts/seed_staging_db.py` is the easiest invocation.

Currently seeds:
- 1 operator user (Brij as owner, `plan_tier='trial'`, `trial_credits_remaining=10`)
- 4 test fans on Twilio's magic numbers, all opted in to `creator_slug='brij-test'`

To add more (different plan tiers, opted-out fan, etc.), edit the `TEST_FANS` list and the operator-user seed block at the top of the script. Re-runs are idempotent — every insert uses `ON CONFLICT DO UPDATE`.

---

## Frontend setup (DONE — for reference / future rebuilds)

The Lovable frontend setup cannot be automated — Lovable's public API doesn't support project duplication or programmatic build-time env var changes (`VITE_*` vars need to be in the bundle at build time, and Lovable's "Cloud Secrets" only inject runtime/Edge-Function secrets, not Vite vars).

What we actually did: **remixed** the prod `zar-fan-connect` project into a new "Zar Staging" project (Lovable doesn't allow branches on the current plan, so a remix is the equivalent), and hardcoded staging URLs into the remix. The remix publishes to its own URL (`zar-chat-magic.lovable.app`) and the prod project (`zar-fan-connect.lovable.app`) is untouched.

If you need to redo this from scratch (e.g. someone deletes the staging remix), use:

### Path A — Remix + hardcode test values via Lovable AI chat (fastest, ~5 min)

In Lovable's AI chat for the existing project, paste this prompt:

```
Create a new branch called "staging". On the staging branch only, edit src/lib/api.ts (or wherever VITE_API_BASE / VITE_STRIPE_PUBLISHABLE_KEY are read from) to hardcode these values:

- API_BASE = "https://operator-production-9330.up.railway.app"
- STRIPE_PUBLISHABLE_KEY = "<pk_test_... value from .env>"
- GOOGLE_CLIENT_ID = "<value from .env>"

Add a visible orange banner at the top of every page that says "STAGING — test mode" so we never confuse it with prod. Don't change anything on main.
```

Lovable will give you a URL like `https://staging--zar-fan-connect.lovable.app`. Send that URL to whoever is testing — that's their full dashboard.

> **Why this is safe:** all three values (`pk_test_...`, Google Client ID, operator URL) are public by design. The Stripe publishable key cannot charge cards on its own, the Google Client ID is exposed in every OAuth redirect anyway, and the operator URL is a URL.

### Path B — Skip the frontend, test backend-only (zero extra work)

If you don't need a UI, everything works via curl + the seed script. See "How to test things without the frontend" below.

### After the frontend is wired (DONE for the current setup)

These four operator env vars are set on `zarna-operator-staging` and pointed at `https://zar-chat-magic.lovable.app`:
- `FRONTEND_URL`
- `STRIPE_CHECKOUT_SUCCESS_URL` → `<frontend>/billing?status=success`
- `STRIPE_CHECKOUT_CANCEL_URL` → `<frontend>/billing?status=cancel`
- `CORS_ALLOWED_ORIGINS`

CORS verified: `OPTIONS /api/auth/me` from `Origin: https://zar-chat-magic.lovable.app` returns `Access-Control-Allow-Origin: https://zar-chat-magic.lovable.app` and `Access-Control-Allow-Credentials: true`.

If a future rebuild creates a frontend at a different URL, swap these four values via Railway's variable upsert API (or just ask the AI in this repo to do it for you).

---

## How to test things without the frontend

> All `psql` examples below use `$STAGING_MAIN_DB_URL` and `$STAGING_OPERATOR_DB_URL`. Set them once in your shell from Brij's 1Password entry "Zarna Staging — DB URLs", or from the Railway dashboard (Postgres → Variables → `DATABASE_PUBLIC_URL`).

### Test 1 — Send a real outbound SMS to your phone

```bash
# 1. Add yourself as a fan
psql "$STAGING_MAIN_DB_URL" <<SQL
INSERT INTO contacts (phone_number, source, creator_slug, fan_name)
VALUES ('+1YOUR_REAL_NUMBER', 'manual', 'brij-test', 'Me')
ON CONFLICT (phone_number) DO NOTHING;
SQL

# 2. Text the staging Twilio number from your phone: text +1 (573) 229-0656
# The bot replies via the brij-test persona.
```

### Test 2 — Force the bot to think it received an SMS (no real phone needed)

```bash
# $STAGING_API_SECRET_KEY = ask Brij (1Password → "Zarna Staging — API_SECRET_KEY")
curl -X POST https://web-production-d7b70.up.railway.app/message \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $STAGING_API_SECRET_KEY" \
  -d '{"phone_number": "+15005550006", "message": "tell me about yourself"}'
# Expect: {"reply": "...", "skipped": false}
```

This bypasses Twilio entirely — useful for fast iteration and CI smoke tests. The reply comes from real Gemini, with the `brij-test` persona loaded.

### Test 3 — Drive credits to zero, see the limit hit

The seed gives you `trial_credits_remaining=10`. Send 11 outbound messages (via test 2 above, repeated) — the 11th should fail with `credit_limit_exceeded`. Inspect:

```bash
psql "$STAGING_OPERATOR_DB_URL" \
  -c "SELECT email, plan_tier, trial_credits_remaining FROM operator_users;"
psql "$STAGING_OPERATOR_DB_URL" \
  -c "SELECT kind, credits, created_at FROM credit_events ORDER BY created_at DESC LIMIT 20;"
```

### Test 4 — Stripe webhook end-to-end

In Stripe test mode dashboard → Developers → Webhooks → click `we_1TVLc9HCxNGsWyPBXmx3NavI` → "Send test webhook" → pick `checkout.session.completed`. Operator receives the event, updates `plan_tier` accordingly, writes a `credit_events` row.

---

## How Team Members Access Staging (e.g. Veer)

The staging environment is a **deployed website**. Team members open a URL in their browser — no API keys, no local setup, no `.env` files required. All secrets live inside Railway's environment, not on anyone's laptop.

### Access Level 1 — Use the staging site (zero setup, browser only)

This is the default for Veer or any dev who wants to test features, create accounts, or verify UI changes.

**What Brij does once:**
1. Share the staging dashboard URL with Veer: **https://zar-chat-magic.lovable.app**
2. Optionally: create a pre-made account for Veer by adding a row to `operator_users` in the staging operator DB, OR have him sign up himself at `/signup`.
3. Tell him: "Use test card `4242 4242 4242 4242` for any Stripe checkout. SMS goes to/from `+1 (573) 229-0656`. The orange banner at the top tells you you're on staging."

**What Veer does:**
- Opens the URL
- Signs up or logs in
- Uses the dashboard exactly like a real customer would
- Creates test accounts, assigns plans, triggers blasts, tests credit limits
- No keys, no config, no asking Brij for access to anything

**What Veer can test this way:**
- Signing up and going through onboarding
- Assigning a plan and seeing credits appear
- Sending a blast and watching credits deduct
- Hitting the credit limit and seeing the error
- Buying a booster pack via Stripe test checkout
- Upgrading/downgrading plans
- Receiving an actual SMS on a real phone (if he's added as a fan in staging DB)
- Verifying UI changes look correct on a real deployed site

---

### Access Level 2 — Run code locally against staging (for developing and testing code changes)

For when Veer is writing code and wants to test it against the staging DB and APIs instead of mocking everything.

**What Brij does:**
Share a `.env.staging` file with Veer privately (Slack DM, Notion, 1Password — wherever you share things). This file has all the staging credentials filled in. It's safe to share with trusted devs — staging keys can't touch real fans or charge real money.

The AI will generate a ready-to-share `.env.staging` file as part of Step 4 when finishing the setup.

**What Veer does:**
```bash
# In the repo root — DO NOT overwrite .env, load .env.staging directly
dotenv -f .env.staging run -- python main.py
# OR for the operator
cd operator && dotenv -f ../.env.staging run -- python main.py
```

> Why not `cp .env.staging .env`? Because if Veer ever gets prod creds in his `.env`, copying staging on top would silently overwrite them. Loading `.env.staging` explicitly per-run is safer and self-documenting.

His local server behaves exactly like the staging Railway deployment, with real DB, real Twilio, real Stripe test mode — just running on his laptop.

> **Note on inbound SMS during local dev:** if Veer is running the main app locally, the Twilio staging webhook still points at the deployed Railway staging URL (set in Step 7), not his laptop. To test inbound SMS against local code, he needs to use `ngrok` or `cloudflared` to tunnel his laptop and temporarily repoint the Twilio webhook. Outbound SMS (and everything else) works fine without this.

---

### Access Level 3 — See Railway logs and deployments (optional)

If Veer needs to debug why something failed in staging:

**What Brij does:**
Go to Railway → Project Settings → Members → Invite Veer by email. Give him Viewer or Member role. (Costs ~$5/seat/month on Railway team plan — see cost table.)

**What Veer gets:**
- Live deployment logs
- Environment variable list (can see keys but not edit them)
- Deploy history
- Service health

---

### The golden rule for staging

> **Staging is designed to be used like prod.** If Veer has to ask Brij "how do I set something up to test X", that means staging is missing something. The goal is that any dev can go to the URL, create an account, and test any feature end to end without any help.

---

## How to Use Staging Once It's Running

### Test: credit limit + plan upgrade
1. Log into staging operator dashboard
2. Create a test user account
3. Set `plan_tier='starter'` directly in staging DB (or sign up via Stripe test checkout with card `4242 4242 4242 4242`)
4. Send messages / trigger blasts until credits hit 3,200
5. Try to send one more — should get `credit_limit_exceeded` 402
6. Buy a booster or upgrade plan via Stripe test checkout
7. Verify credits updated in DB + `credit_events` audit row written

### Test: blast to your phone
1. Staging Twilio number is `+1 (573) 229-0656`
2. Set yourself as a fan in staging DB
3. Trigger a blast from the staging operator dashboard
4. SMS arrives on your phone — verify content, check credits deducted

### Test: Stripe billing cycle reset
1. Create a Stripe test clock in Stripe dashboard (test mode)
2. Create a test customer attached to the clock
3. Subscribe them to a plan
4. Advance the clock +31 days via Stripe API
5. Stripe fires `invoice.payment_succeeded` → webhook handler resets `credits_used` to 0
6. Check DB for `KIND_PLAN_RESET` row in `credit_events`

---

## Cost to Run Staging

Railway charges per service running 24/7 (rough $5/service minimum), not per project.

| Resource | Monthly cost |
|---|---|
| Railway `zarna-staging` app service | ~$5 |
| Railway `zarna-staging` Postgres | ~$5 |
| Railway `zarna-operator-staging` app service | ~$5 |
| Railway `zarna-operator-staging` Postgres | ~$5 |
| Railway team seat for Veer (if added to project) | ~$5 |
| Twilio number `+15732290656` | $1.15 + pennies per test SMS |
| Stripe test mode | $0 |
| Lovable staging preview | $0 (included in existing Lovable plan) |
| **Total** | **~$25–30/month** |

If cost is a concern, Railway services can be configured to scale to zero when idle — but cold starts make Stripe webhooks unreliable, so it's not recommended.

---

## Day-to-day usage

### Deploy a code change to staging

```bash
git checkout staging
git merge feat/your-branch          # or cherry-pick specific commits
git push origin staging
# Railway auto-deploys both web + operator services within ~3 minutes
```

Watch the deploy:
```bash
# Quick: open the Railway UI and watch logs
# Or via CLI: railway logs --service web --environment staging
```

### Reset the staging DB to a clean slate

```bash
# Wipe and re-seed (full reset). Set the env vars from 1Password first.
psql "$STAGING_MAIN_DB_URL" \
  -c "TRUNCATE contacts, messages CASCADE;"
psql "$STAGING_OPERATOR_DB_URL" \
  -c "TRUNCATE operator_users, credit_events CASCADE;"

# Then re-seed (see Step 9 above for the full command)
```

### Check what's deployed

```bash
# Latest commit deployed
git log origin/staging -1 --oneline

# Service status from the Railway API
source .env
curl -s -X POST https://backboard.railway.app/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ service(id: \"0d3be54b-3b8c-433f-9224-e51f52dca732\") { deployments(first: 1) { edges { node { status createdAt } } } } }"}'
```

---

## If you (or future-AI) need to rebuild this from scratch

All the resource IDs above will be invalid. Use the **Detailed step reference** above as a checklist — every step has the exact GraphQL mutations / Stripe API calls / Twilio API calls used. The full automation script lives in this conversation's transcript and can be regenerated by asking the AI:

> "Rebuild the staging environment using the same plan as `docs/plans-to-complete/08_staging_environment_setup.md` — start by creating fresh Railway projects."

The only inputs the AI needs are the API keys in `.env` (Railway, Twilio, Stripe) — all of which are already there.
