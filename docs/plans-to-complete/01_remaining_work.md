# Plan: Remaining Work Tracker
_Source: `docs/engineering/remaining_work.md`_

Every item is still `[ ]`. Build in this order:

## 1. Stripe Billing (blocks revenue)
### Backend
- [ ] Add `stripe` to `operator/requirements.txt`
- [ ] Add `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PUBLISHABLE_KEY` to `.env` and Railway
- [ ] Create Stripe products + prices for all tiers (performer + business, monthly + annual)
- [ ] Add `plan_tier`, `stripe_customer_id`, `stripe_subscription_id`, `billing_cycle_anchor` to `operator_users`
- [ ] `POST /api/billing/create-checkout-session`
- [ ] `POST /api/billing/portal`
- [ ] `POST /api/billing/webhook` — handles `checkout.session.completed`, `invoice.paid`, `customer.subscription.deleted`
- [ ] `GET /api/billing/status`
- [ ] Add `operator_credit_usage` table
- [ ] Gate blast sending behind credit check in `/api/blasts`

### Lovable
- [ ] Plan selection page after onboarding
- [ ] Billing/account page — current plan, credits used, renewal date
- [ ] Credit top-up pack purchase flow
- [ ] Annual billing toggle
- [ ] Upgrade prompt at 80% credits

## 2. Onboarding Gate on All Pages (quick — 30 min Lovable prompt)
### Lovable
- [ ] Apply onboarding redirect to: `/blasts`, `/shows`, `/inbox`, `/my-bot`, `/settings`
- [ ] Any unauthenticated user hitting any app route → redirect to `/login`

## 3. Credit Tracking UI
### Backend
- [ ] `GET /api/billing/status` — include `credits_used`, `credits_total`, `period_end`
- [ ] Increment `operator_credit_usage` on every SMS sent/received

### Lovable
- [ ] Credit usage bar in dashboard header
- [ ] Warning banner at 80% usage
- [ ] Full credits breakdown on billing page

## 4. Auto-Provisioning (needs Twilio campaign approval first)
### Backend
- [ ] Add `phone_number` column to `operator_users`
- [ ] Add `TWILIO_MESSAGING_SERVICE_SID` to `.env` and Railway (waiting on campaign approval)
- [ ] Write `operator/app/provisioning.py` module
- [ ] Call `provision_tenant()` at end of `api_onboarding_submit`
- [ ] Add `provisioning_status` column to `bot_configs`
- [ ] `GET /api/provisioning/status`

### Lovable
- [ ] Provisioning spinner after onboarding completes
- [ ] Show dedicated phone number on dashboard once provisioned
- [ ] Handle provisioning failure state with retry button

## 5. Team Seats Management
### Backend
- [ ] Add `team_members` table
- [ ] `POST /api/team/invite`
- [ ] `POST /api/team/accept`
- [ ] `DELETE /api/team/remove/:id`
- [ ] `GET /api/team/members`
- [ ] Enforce seat limit by plan tier

### Lovable
- [ ] Team management page (business accounts only)
- [ ] Seat limit indicator
- [ ] Invite email flow

## 6. Smart Send UI
### Backend
- [ ] Add `engagement_score` or `last_replied_at` to contacts
- [ ] `GET /api/contacts/engaged?top=N`

### Lovable
- [ ] Smart send toggle in blast composer
- [ ] Audience size selector + estimated credits
- [ ] Show credit savings vs sending to all

## 7. Account Settings Page
### Lovable
- [ ] Change display name, email, password
- [ ] Connect/disconnect Google OAuth
- [ ] Danger zone: delete account

## 8. FAQ Page (Marketing site)
### Lovable
- [ ] Standard FAQ section covering credits, plans, AI voice, carriers, contracts
