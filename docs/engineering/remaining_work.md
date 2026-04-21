# Zar — Remaining Work Tracker
_Last updated: Apr 19, 2026_

---

## 1. Stripe Billing (Highest Priority — blocks revenue)

### Backend (me)
- [ ] Add `stripe` to `operator/requirements.txt`
- [ ] Add `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PUBLISHABLE_KEY` to `.env` and Railway
- [ ] Create Stripe products + prices for all tiers (performer + business, monthly + annual)
- [ ] Add `plan_tier`, `stripe_customer_id`, `stripe_subscription_id`, `billing_cycle_anchor` columns to `operator_users`
- [ ] New route: `POST /api/billing/create-checkout-session` — creates Stripe Checkout session for plan selection
- [ ] New route: `POST /api/billing/portal` — opens Stripe Customer Portal for plan changes/cancellation
- [ ] New route: `POST /api/billing/webhook` — handles `checkout.session.completed`, `invoice.paid`, `customer.subscription.deleted` events; updates plan tier in DB
- [ ] New route: `GET /api/billing/status` — returns current plan, credits used this month, renewal date
- [ ] Add credit tracking table: `operator_credit_usage (user_id, period_start, credits_used, updated_at)`
- [ ] Gate blast sending behind credit check in `/api/blasts` endpoints

### Lovable (prompt needed)
- [ ] Plan selection page after onboarding — show performer or business tiers, connect to Stripe Checkout
- [ ] Billing/account page — current plan, credits used this month (progress bar), renewal date, "Upgrade" and "Manage billing" buttons
- [ ] Credit top-up pack purchase flow (one-time Stripe payment)
- [ ] Annual billing toggle wired to actual Stripe annual prices
- [ ] Upgrade prompt when user hits 80% of credits

---

## 2. Auto-Provisioning (Step 5 of business flow — blocks self-serve)

### Backend (me)
- [ ] Add `phone_number` column to `operator_users`
- [ ] Add `TWILIO_MESSAGING_SERVICE_SID` (platform campaign SID) to `.env` and Railway — **waiting on Twilio campaign approval**
- [ ] Write `operator/app/provisioning.py` module:
  - `provision_tenant(user_id)` — buys Twilio number, sets webhook, writes bot config, stores number
  - `buy_twilio_number(area_code)` — purchases number via Twilio API, assigns to messaging service
  - `configure_webhook(phone_sid, tenant_slug)` — sets sms_url to `/smb/inbound?tenant=<slug>`
  - `write_bot_config(tenant_slug, config_json)` — generates `creator_config/<slug>.json` from onboarding data using `smb_template.json`
  - `send_welcome_email(user_email, phone_number)` — sends "you're live" email via Resend
- [ ] Call `provision_tenant()` at the end of `api_onboarding_submit` (async via thread)
- [ ] Add `provisioning_status` column to `bot_configs` (`pending` → `provisioned` → `failed`)
- [ ] Add `GET /api/provisioning/status` endpoint so frontend can poll during provisioning

### Lovable (prompt needed)
- [ ] Show provisioning spinner after onboarding completes ("Setting up your bot…")
- [ ] Show dedicated phone number prominently on dashboard once provisioned ("Your Zar number: (555) 123-4567")
- [ ] Handle provisioning failure state gracefully with retry button

---

## 3. Credit Tracking UI (needed to make credits model make sense)

### Backend (me)
- [ ] `GET /api/billing/status` (see Stripe section) — include `credits_used`, `credits_total`, `period_end`
- [ ] Increment `operator_credit_usage` on every SMS sent/received (hook into Twilio webhook handler)

### Lovable (prompt needed)
- [ ] Credit usage bar in dashboard header or sidebar: "1,240 / 3,200 credits used · resets in 12 days"
- [ ] Warning banner at 80% usage: "You're running low on credits — upgrade or top up"
- [ ] Full credits breakdown on billing page (blasts used X, replies used X, organic used X)

---

## 4. Onboarding Gate on All App Pages

### Lovable (prompt needed)
- [ ] Currently only `/dashboard` redirects to `/onboarding` if setup not complete
- [ ] Apply same check to: `/blasts`, `/shows`, `/inbox`, `/my-bot`, `/settings`
- [ ] Any unauthenticated user hitting any app route should redirect to `/login`

---

## 5. Team Seats Management (Business accounts)

### Backend (me)
- [ ] Add `team_members` table: `(id, owner_user_id, member_email, role, invited_at, accepted_at)`
- [ ] `POST /api/team/invite` — send invite email via Resend
- [ ] `POST /api/team/accept` — accept invite, link member to owner's account/tenant
- [ ] `DELETE /api/team/remove/:id` — remove team member
- [ ] `GET /api/team/members` — list current team
- [ ] Enforce seat limit based on plan tier in invite endpoint

### Lovable (prompt needed)
- [ ] Team management page (business accounts only): list members, invite by email, remove
- [ ] Seat limit indicator: "3 of 4 seats used"
- [ ] Invite email flow (accept link → create account → lands on dashboard as team member)

---

## 6. Smart Send UI

### Backend (me)
- [ ] Add `engagement_score` or `last_replied_at` to contacts table so we can rank fans
- [ ] `GET /api/contacts/engaged?top=N` — returns top N most engaged fans

### Lovable (prompt needed)
- [ ] Smart send toggle in blast composer: "Send to all fans" vs "Send to top engaged fans"
- [ ] When smart send is on: show audience size selector ("Top 100 / 200 / 500 fans") and estimated credits used
- [ ] Show credit savings: "Saves you ~1,200 credits vs sending to all 1,500 fans"

---

## 7. Account Settings Page

### Lovable (prompt needed)
- [ ] Change display name, email, password
- [ ] Connect/disconnect Google OAuth
- [ ] Danger zone: delete account

---

## 8. FAQ Page (Marketing site)

### Lovable (prompt needed)
- [ ] Standard FAQ section on the marketing site covering:
  - What is a credit?
  - Can I change plans?
  - How does the AI learn my voice?
  - What happens when I run out of credits?
  - Can fans text from any carrier?
  - Is there a contract?

---

## Pricing Reference (locked in Apr 2026)

### Performer tiers
| Plan | Price | Credits | Good for |
|---|---|---|---|
| Starter | $79/mo | 3,200 | ~700 fans |
| Growth | $149/mo | 6,200 | ~1,500 fans |
| Pro ★ | $299/mo | 12,500 | ~3,000 fans |
| Scale | $599/mo | 25,200 | ~6,000 fans |
| Elite | $999/mo | 41,900 | ~10,000 fans |
| Creator | $1,999/mo | 80,300 | ~20,000 fans |

### Business tiers
| Plan | Price | Credits | Good for | Seats |
|---|---|---|---|---|
| Essentials | $49/mo | 1,900 | ~400 customers | 2 |
| Standard ★ | $99/mo | 4,300 | ~1,200 customers | 4 |
| Pro | $199/mo | 9,000 | ~2,500 customers | Unlimited |

### Credit add-on packs (both plan types)
| Pack | Credits | Price |
|---|---|---|
| Mini boost | 500 | $12 |
| Blast pack | 1,500 | $32 |
| Big send | 4,000 | $79 |
| Power pack | 10,000 | $179 |

### Key pricing rules
- 1 credit = 1 SMS sent or received
- Cost per credit: ~$0.0125 (Twilio + AI blended)
- Target gross margin: 40% (higher on business plans ~60-75%)
- Reply cap: 20 exchanges per fan per blast thread
- Annual billing: price × 10 (2 months free, 17% off)
- Margins improve ~15% once Twilio bulk rates kick in at 20+ clients

---

## Twilio Campaign Status
- ✅ Brand registered ("My first Twilio account")
- ✅ Existing Zarna Garg campaign (Social use case) — for Zarna's personal bot only
- ⏳ New platform campaign (Low Volume Mixed) — **submitted, awaiting approval (1-2 weeks)**
- Once approved: add `TWILIO_MESSAGING_SERVICE_SID` to `.env` and Railway, then build auto-provisioning

---

## Build Order Recommendation
1. **Stripe billing** — can't charge anyone without it
2. **Onboarding gate on all pages** — quick Lovable prompt, 30 min
3. **Credit tracking UI** — makes the pricing model visible to users
4. **Auto-provisioning** — unblocks fully self-serve signups (needs Twilio campaign approval first)
5. **Team seats** — needed before first business client
6. **Smart send UI** — nice to have for launch
7. **FAQ page + account settings** — polish
