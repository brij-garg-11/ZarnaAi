# 00 — START HERE: Veer's Access & Safety Guide

_Owner: Brij_
_Audience: Veer (`veergarg1116@gmail.com`, GitHub: `VeerGarg11`)_
_Last updated: May 11, 2026_

**Read this entire document before doing anything else.** It tells you what you have access to, what you must absolutely not touch, and what to do if something breaks.

After this, your testing playbook is at [`04_veer_testing_playbook.md`](04_veer_testing_playbook.md).

---

## The one rule

> Everything you do is on **staging**. You never have production access for anything that could write data, send messages, or charge money. If you ever feel like you're about to touch prod by accident, **stop and DM Brij**.

The **orange "STAGING" banner** at the top of every dashboard page is your seatbelt. If you don't see it, you're not on staging — back out.

---

## CRITICAL — Things you MUST NOT do

Read this table once now, then re-read before any phase that touches data, money, or infrastructure. **Every item below has bitten someone in industry. Don't make it bite us.**

| Forbidden action | Why it would be catastrophic |
|---|---|
| **Switch Stripe Dashboard from Test to Live mode** | You'd be operating against real customer cards. Refunds, charges, subscription cancellations would hit real people |
| **Run any script with `STAGING_MAIN_DB_URL` set to a prod URL** (`web-production-7b91...`, `api.zar.bot`, etc.) | The script would TRUNCATE / DELETE / re-seed prod data. We'd lose Zarna's actual fans |
| **Run `scripts/reset_staging_db.py` without verifying the URL it points at** | Same as above — accidentally pointing it at prod = data wiped |
| **Run any cron script (`scripts/sync_*`, `scripts/score_*`, `scripts/drip_*`) with prod env vars** | Would write to live customer DBs / Notion / SMS pipelines |
| **Push directly to `main`** (`git push origin main`) | Skips review. The branch is protected so it should fail, but don't even try |
| **`git push --force` on any shared branch** | Rewrites history, breaks every other contributor |
| **Merge your own PRs without Brij's review** | The whole point of this workflow is human review of what hits prod |
| **Edit `app/storage/postgres.py` migrations directly** | Bad migrations corrupt fan data across all clients |
| **Edit any `railway.*.toml` file** | Cron schedules run against live prod databases |
| **Edit any `.env` file or commit secrets** | Even on staging — credentials in git history leak permanently |
| **Edit `creator_config/zarna.json` or any `training_data/zarna_*` file** | Would change how Zarna's bot replies to her real fans on every text |
| **Touch the Twilio main account (parent), not the staging subaccount** | Could affect real fan SMS delivery, real A2P 10DLC registration, real billing |
| **Send SMS from a real Twilio number to a real human's phone** during testing | Even harmless test text = TCPA violation if they didn't consent |
| **Buy real Twilio numbers** (in either prod or staging) without Brij's say-so | Each costs money + needs A2P registration |
| **Delete or rename anyone's Notion CRM page** in the Zar CRM workspace | Loses customer record + history |
| **Issue refunds in Stripe (test mode is fine; live mode is forbidden)** | Live mode = real customer-facing reversal |
| **Use the Stripe Customer Portal as if you were the customer** in live mode | Cancels their actual subscription |
| **Approve / reject the A2P 10DLC campaign in Twilio** | Could deactivate the production sending |
| **Run any DDL (`DROP`, `TRUNCATE`, `ALTER`, `CREATE TABLE`) directly on any database** | Use migrations only, and only on staging, and only with Brij confirming |
| **Touch `creator_config/west_side_comedy.json`** | Live customer config |

If you ever find yourself unsure whether something is on the list — assume yes, ping Brij.

---

## What you have access to

Brij has set these up for you. If any of these don't work when you try, ping Brij.

### 1. Staging operator account (super-admin)
- **URL**: https://zar-chat-magic.lovable.app
- **Email**: `veergarg1116@gmail.com`
- **Password**: Brij will share via 1Password / DM (a secure random password starting with `Zar-`)
- **Sign-in options**: Email + password OR "Continue with Google" — both work for the same account
- **Permissions**: Super-admin on staging only — you can switch into any of the 3 seeded accounts (brij-test, alice-test, westside-test) using the project switcher
- **Important**: Change the temp password on first login. Use the Account settings page → "Change password".

### 2. GitHub repository (`brij-garg-11/ZarnaAi`)
- **Your username**: `VeerGarg11`
- **Permission**: Write (push)
- **Action needed**: You'll get a collaborator invite email from GitHub. Accept it.
- **What you can do**: Create branches, push commits, open pull requests
- **What you can't do**: Push directly to `main` (branch protection is on), force-push, delete branches you didn't create, rewrite history

### 3. Cursor (you already have this)
- For all the testing prompts in `04_veer_testing_playbook.md`
- Use it as your AI pair when reading code or fixing bugs

### 4. Lovable project — `zar-chat-magic` (staging frontend ONLY)
- **URL**: https://zar-chat-magic.lovable.app
- Brij will invite you to the Lovable project
- **You can edit**: Frontend UI on staging (the orange-banner dashboard)
- **You CANNOT touch**: `zar-fan-connect` (the production frontend) — it's a separate Lovable project Brij owns

### 5. Twilio Console — staging subaccount only
- Brij will add you as a sub-user under the Zarna AI Twilio parent account
- **Subaccount**: "Zarna Staging"
- **Numbers visible**: `+1 (573) 229-0656` (brij-test), and the alice-test number
- **You CANNOT see**: The parent account's main number, billing details, or A2P registration for prod

### 6. Stripe Dashboard — TEST MODE ONLY
- Brij will invite you with the **Developer** role
- **You can do**: View test webhooks, replay test webhooks, trigger test events via CLI, view test customers, cancel test subscriptions
- **You CANNOT switch to**: Live mode (the Test/Live toggle is locked for your role)
- **You will need**: Stripe CLI installed locally for Phase 6 + Phase 21. Install: https://stripe.com/docs/stripe-cli

### 7. Notion CRM workspace
- Brij will invite you as a guest to the Zar CRM page
- **You can do**: Read every customer page, the Costs database, the Performers / Businesses databases
- **You CAN'T**: Delete pages, rename databases, archive properties

### 8. Railway dashboard — read-only on prod, full on staging
- Brij will invite you to the Railway team
- **Full access**: `zarna-staging`, `zarna-operator-staging` projects (you can deploy, read logs, edit env vars, restart services)
- **Read-only**: `Zar` (production) project — you can read logs and check service status, but you can't deploy, edit env vars, or restart services

### 9. 1Password — Zarna Staging vault only
- Brij will share the "Zarna Staging" vault with you
- **Contains**: Staging API keys, staging DB passwords, your temp staging operator password
- **Does NOT contain**: Any production credentials, live Stripe key, prod Twilio token, any customer secrets

### 10. Slack — Zarna AI workspace
- Channel: `#zarna-engineering` (you'll be added)
- **Use this** for any blocker or unclear test
- **Do not use email** for urgent things

### 11. Your real personal phone
- Used for real-inbound SMS testing in Phase 10 and Phase 19
- Brij will tell you when to send a real text to which staging Twilio number
- **Don't send SMS to staging numbers from your work / shared phones** — keep it personal

---

## What you DO NOT have access to (and why)

| Resource | Why you don't have it |
|---|---|
| Production database (write) | One bad query wipes Zarna's 4,500-fan production data |
| Production database (direct psql) | Read-only is technically possible but not granted by default — ask Brij if a phase needs it |
| Live Stripe mode | Real customer cards, real refund power, real subscription cancellation power |
| Live Twilio account / parent account | A2P registration, real outbound throughput, real billing |
| `OPERATOR_BOOTSTRAP_EMAIL` / bootstrap passwords for prod | Could overwrite the prod owner account |
| Production env vars on Railway (write) | Wrong env var = prod outage |
| Production Resend API key | Could email the entire customer base accidentally |
| Production Notion API token | Could write/delete real customer pages |
| Lovable production project (`zar-fan-connect`) | Customer-facing UI — Brij ships this |
| `creator_config/zarna.json`, `creator_config/west_side_comedy.json` | Live customer voice configs |
| `training_data/zarna_*`, `training_data/west_side_comedy_*` | Live customer knowledge bases |
| Any AWS / GCP / Google Cloud account | Not needed for testing |
| GitHub repo settings (branch protection, secrets, webhooks) | Repo admin only |
| GitHub Actions secrets | Used by CI — not editable |

If a phase tells you to do something and you don't have access, that's intentional — flag it, skip it, and Brij will run it.

---

## Pre-action safety checklist

**Before any of these actions, walk through this checklist.** Takes 30 seconds. Saves your life.

### Before running ANY script from the `scripts/` directory

```
[ ] Did I read the script's docstring at the top?
[ ] What env vars does it need? (Read them in the docstring.)
[ ] Are those env vars currently set to staging values? (Run `printenv | grep STAGING`)
[ ] If the script says "operator URL" or "main DB URL", does my env var contain
    "production-1e62" or "operator-production-9330" or "staging" in the name?
    (= staging) — NOT "production-7b91" or "api.zar.bot" (= prod)
[ ] Is there a --dry-run flag I should use first?
[ ] Did I commit my current work in case the script breaks something?
```

### Before running `psql` or any direct DB query

```
[ ] What URL am I connecting to? (The host should NOT be the prod Postgres.)
[ ] Am I about to run a SELECT (safe) or DELETE / UPDATE / DROP (dangerous)?
[ ] If dangerous: did I run it with `BEGIN;` first so I can ROLLBACK?
[ ] Am I 100% sure the WHERE clause restricts to my creator_slug?
```

### Before merging a PR or pushing code

```
[ ] Did I run `git status` and confirm I'm on the right branch?
[ ] Are my changes staging-only (e.g. seed scripts) → PR to `staging`
    OR also-prod-affecting (e.g. brain bug fix) → PR to `main`?
[ ] If PR to `main`: did I tag Brij for review and wait for approval?
[ ] Did I never use `--force` or `--no-verify`?
```

### Before using Stripe Dashboard

```
[ ] Is the toggle in the top-left set to "Test mode"? (Should be orange/yellow.)
[ ] If I see a red "LIVE" indicator anywhere — STOP. I'm not supposed to be in
    live mode. Sign out and DM Brij.
```

### Before sending a blast on staging

```
[ ] Are all recipients on Twilio magic numbers (`+1500...`) or RFC-reserved
    (`+1555...`) — i.e. fake numbers that won't deliver?
[ ] If I'm including my real phone number, am I texting MY OWN number, not
    someone else's?
[ ] Have I confirmed the staging operator URL contains "1e62" or "alice-test"?
```

---

## If you accidentally break something

**Speed matters more than embarrassment. DM Brij in Slack immediately.** Then:

| Situation | What to do |
|---|---|
| Pushed code to `main` directly (somehow) | DM Brij. Don't try to revert yourself. |
| Force-pushed something | DM Brij IMMEDIATELY (others may have based work on it) |
| Ran a script that wiped staging DB | DM Brij. Use `scripts/reset_staging_db.py` to re-seed. No long-term damage. |
| Ran a script that touched PROD DB | DM Brij IMMEDIATELY. **Don't run anything else.** Brij will need to assess + restore from backup |
| Issued a real Stripe refund (live mode) | DM Brij IMMEDIATELY. The customer's bank already knows. |
| Sent a real SMS to a real fan | DM Brij IMMEDIATELY. Document what was sent + when |
| Deleted a Notion page | DM Brij. Notion has 30-day undelete — recoverable. |
| Broke staging frontend (Lovable) | DM Brij. Roll back via Lovable's history. No customer impact. |
| Bot started replying with garbage in staging | DM Brij. Check `creator_config/brij-test.json` for accidental edit. |
| Anything that COULD have hit prod and you're not sure | DM Brij. "I might have done X, can you check?" is always the right move. |

The pattern is: **fast notification beats slow self-rescue.** Brij would rather be pinged 10 times for nothing than once too late.

---

## How to ask for help

In Slack `#zarna-engineering`:

```
@Brij — Phase X, Test ID PLN-XX
What I tried: <one-liner>
What I expected: <one-liner>
What happened: <one-liner>
Screenshot / log: <attach>
Question: <one-liner>
```

If it's blocking your phase, label the message **🚨 blocked**.

If it's a question that can wait, label it **❓ async** — Brij will get to it within a day.

For anything you think might be an emergency (you broke something, you're seeing live data when you shouldn't), label it **🆘 emergency** and DM directly.

---

## Setup checklist (do this before Phase 0)

Tick each one. If anything fails, ping Brij.

- [ ] You received your temp staging operator password from Brij
- [ ] You logged into https://zar-chat-magic.lovable.app and saw the orange STAGING banner
- [ ] You changed your temp password to a real one in Account settings
- [ ] You accepted the GitHub collaborator invite for `brij-garg-11/ZarnaAi`
- [ ] You cloned the repo locally: `git clone https://github.com/brij-garg-11/ZarnaAi.git`
- [ ] `cd ZarnaAi && git checkout staging && git pull` works
- [ ] Cursor opens the repo without errors
- [ ] You accepted the Lovable invite to `zar-chat-magic`
- [ ] You accepted the Twilio sub-user invite (Zarna Staging subaccount)
- [ ] You accepted the Stripe Developer invite (TEST MODE ONLY)
- [ ] You accepted the Notion guest invite to the Zar CRM page
- [ ] You accepted the Railway team invite
- [ ] You accepted the 1Password "Zarna Staging" vault share
- [ ] You joined the Slack `#zarna-engineering` channel
- [ ] You installed Stripe CLI: `brew install stripe/stripe-cli/stripe` (or per https://stripe.com/docs/stripe-cli)
- [ ] You installed `psql`: `brew install libpq && brew link --force libpq`
- [ ] You read the entire `04_veer_testing_playbook.md` once end-to-end (even if you don't understand every test yet — just get the lay of the land)
- [ ] You have access to your real personal phone for SMS testing

Once all checked, open `04_veer_testing_playbook.md` and start Phase 0.

---

## Reference — what's in this folder

| File | Purpose |
|---|---|
| `00_START_HERE.md` | This file — access + safety |
| `01_task_ux_and_flow_review.md` | Older review (not part of this testing series) |
| `02_task_credit_transparency.md` | Older review (not part of this testing series) |
| `03_comprehensive_test_plan.md` | The full test plan — every test ID lives here. Reference doc. |
| `04_veer_testing_playbook.md` | Your turn-by-turn workflow. The "what to do" doc. |
| `phase-reports/P00-...md` through `P28-...md` | Your phase reports as you finish each phase |
| `phase-reports/SUMMARY.md` | Final wrap-up after Phase 28 |

---

Welcome to the team, and thank you for doing this. Once you're done, Zarna AI moves from "works for one customer" to "actually safe to onboard a second."
