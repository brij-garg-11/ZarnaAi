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

## 4. Auto-Provisioning ✅ COMPLETED (Apr 22, 2026)
### Backend — DONE
- [x] `phone_number` column on `operator_users` — already existed
- [x] `operator/app/provisioning/` module (orchestrator + config_writer + ingestion + notifications + phone)
- [x] Async `provision_new_creator()` called at end of `api_onboarding_submit` for performer accounts
- [x] `provisioning_status` + `error_message` columns on `bot_configs`
- [x] `GET /api/provisioning/status` endpoint live
- [x] `creator_configs` + `creator_embeddings` (vector 3072, HNSW halfvec index) tables live on Railway
- [x] Multi-tenant RAG isolation: PgRetriever, slug-scoped contacts, slug-scoped winning examples
- [ ] Unstub `phone.py` (PROVISIONING_PHONE_MODE=real) — **blocked on Twilio A2P campaign approval**
- [ ] Per-number routing in `main.py` (phone → slug → brain) — **blocked on Twilio A2P**

### Lovable — PENDING (unblocked, can build now)
- [ ] Provisioning spinner after onboarding completes (polls `GET /api/provisioning/status`)
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
