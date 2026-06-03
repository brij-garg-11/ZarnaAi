# Plan: Convert Platform to B2B "Request Access" Model

_Created: Jun 3, 2026_
_Status: **Backend implemented + tested** (Jun 3, 2026). The operator service now has the `access_requests` table, public `POST /api/access-request`, admin approve/reject (JSON + Operator-HQ "Leads" page), `create_bot_for_user` with unified performer+business provisioning, closed public signup, locked `onboarding/submit`, Notion lead push + email alerts, set-password invite, and a `PLATFORM_BRAND` constant. 17 operator tests pass against a real Postgres. **Remaining: frontend** — submit the Lovable prompts in `docs/engineering/lovable_prompts_b2b.md`, then cut over the domain/brand._

---

## What This Is

Today the platform is built as **self-serve SaaS**: anyone can sign up, run the 4-step onboarding wizard, and a bot is provisioned automatically (number bought, personality generated, RAG ingested, trial credits seeded) with zero human in the loop.

That model no longer fits the business. We are now a **B2B, white-glove, managed service** for named creators (Zarna, Matthew Berry, and the like). Two forces drive the change:

1. **A2P / carrier reality** — a creator's bot represents *their* brand. Premium clients want their own Twilio Brand + Campaign registration (their name on the opt-in), which is a 2–3 week carrier-reviewed process we run on their behalf. This cannot be instant self-serve.
2. **Deal shape** — clients at this tier expect a conversation, qualification, and a managed setup — not a wizard. We want to curate who gets on the platform.

**The shift:** "Create a bot" stops being an instant self-serve action and becomes a **request** ("Apply" / "Request access"). We review the request, have the conversation, and then *we* trigger the existing provisioning pipeline on the client's behalf. Existing customers keep logging in and seeing their dashboards exactly as they do today.

This plan does **not** throw away the self-serve machinery — the entire provisioning pipeline (`provision_new_creator`) is solid and stays. We are inserting an **approval gate** in front of it and re-pointing the public "create a bot" funnel at an application form.

---

## Research: How Comparable B2B Platforms Do It (this is typical)

| Platform | Front door | Self-serve signup? | Notes |
|---|---|---|---|
| **Community.com** (closest comparable — creator SMS, $150M+ raised) | **"Log In \| Get a Demo"** only | ❌ No | Sales-led. Carrier Brand/Campaign registration handled by Community as the platform. Demo → conversation → managed onboarding. |
| **2026 B2B SaaS norm** (per industry benchmarks) | Short conversational/application intake (≤3 fields) → routing | Hybrid | Enterprise/high-intent leads → concierge/sales; only low-tier segments get instant self-serve. Demo-request forms replaced by short intake + human follow-up within ~24h. |
| **SlickText (10DLC path)** | "Contact your rep" | ❌ No (for 10DLC) | Self-serve only exists for their simplest toll-free tier; local-number provisioning is request-based. |

**Conclusion:** A request/apply front door with login preserved for existing customers is the standard pattern for exactly our segment. We are not inventing anything unusual — we are aligning with how the category leader (Community.com) already operates.

Key principle from the research we will follow: **keep the application short** (abandonment rises sharply past ~3 fields), and **route high-intent leads to a human within 24h** rather than forcing a scheduled-call gate.

---

## Current State (verified in code — do NOT rebuild)

### Frontend (`lovable-frontend/`, React + Vite + cookie-session auth against operator API)
- Router: `src/App.tsx`. Public marketing pages, `/login`, `/signup`, `/onboarding`, and protected `/:slug/*` dashboard routes under `SlugGuard` + `RequireOnboarding`.
- **The create-bot funnel is centralized** in `src/lib/create-bot.ts` (`useCreateBot()`): checks `/api/onboarding/status` → unauth → `/signup`; completed → dashboard; else → `/onboarding`. Almost every "Create My Bot" CTA calls this one hook.
- CTAs that call `createBot()`: `Navbar`, `CTA`, `Performers`, `Business`, `HowItWorks`, `PricingGrid` (enterprise "Contact us").
- Paid-tier path: `src/lib/start-checkout.ts` (signup → onboarding → Stripe), used by `PricingGrid` and `Plans`.
- Onboarding wizard: `src/pages/Onboarding.tsx` (4 steps → `POST /api/onboarding/submit` → poll `/api/provisioning/status`).
- Dashboards: `src/pages/Dashboard.tsx` + `DashboardShell`; `ProvisioningBanner` polls provisioning status.
- An **`EarlyAccessDialog`** already exists (mailto-only, currently dead on main paths) — a natural shell to repurpose.
- API base: `src/lib/api.ts` (`VITE_API_BASE`, `credentials: "include"`).

### Backend (`operator/`, Flask)
- `POST /api/auth/signup` — open self-serve, creates a bare `operator_users` row (no slug), returns `onboarding_required: true`. No CAPTCHA/approval.
- `POST /api/onboarding/submit` (`operator/app/routes/api.py` ~L4847–5046) — **the thing that auto-creates everything**: upserts `bot_configs` (`status='submitted'`), sets `operator_users.creator_slug`+`account_type`, seeds 1,000 trial credits, inserts team owner, creates Notion CRM row, and (for `performer`) starts the `provision_new_creator` background thread.
- `provision_new_creator` (`operator/app/provisioning/__init__.py`) — phases: phone → config (Gemini) → RAG ingestion → welcome email → status `live`. Tracked via `bot_configs.provisioning_status`.
- `GET /api/onboarding/status` — binary `completed = bool(creator_slug AND account_type)`.
- `GET /api/provisioning/status` + `POST /api/provisioning/retry`.
- **No waitlist / lead / application / approval table exists.** Notion CRM is written *after* bot creation, so it cannot gate anything today.
- The single gate for all tenant access is `creator_slug` + `account_type` on `operator_users` — a lead with no slug already sees "not onboarded" everywhere.

---

## Target State

```
Public visitor
   └─ Marketing site (no pricing, managed-service messaging, new CTA)
        └─ "Apply for access" / "Request a demo"  (short form, ≤4 fields)
             └─ POST /api/access-request   → access_requests row (status='new')
                                            → Notion lead + ops alert
                  └─ "Thanks — we'll reach out within 24h" screen
                     (NO account created)

Operator (Brij) reviews lead → conversation → (may request info) → decision
   └─ Admin approve  → creates operator_users account for the client
                      → operator fills in all bot details (we build it for them)
                      → triggers provision_new_creator immediately (Standard)
                      → emails client an invite to set a password

Client (first time)
   └─ Clicks invite → set password (signup-via-invite) → lands on dashboard
        └─ Dashboard + ProvisioningBanner ("Setting up…" → "Live + number")

Returning client / existing customer (Zarna, etc.)
   └─ Logs in normally → /:slug/dashboard   (UNCHANGED)
```

**Existing customers (Zarna, etc.):** zero change. They log in, they see `/:slug/dashboard`. Nothing in the auth or dashboard path is touched.

### Tier model (current)
- **Standard only, for now.** Every approved client attaches to the shared `twowaybot` platform Brand/Campaign and provisions immediately on approval. No tier selection, no pricing surfaced.
- **Premium / per-client A2P** (dedicated Brand/Campaign in the creator's own name, 2–3 week carrier setup) is **future work** tracked in plan 09 — not part of this milestone.

---

## Decisions (LOCKED)

1. **Pattern: Option A — "lead-first, account created at build time."** The public "Apply" form creates **only** an `access_requests` lead row — no `operator_users` account. We review, then *we* build the bot for the client (create account + bot + provisioning) and send them an invite to set a password. No "pending account" state to manage in the UI. (Option B and Option C remain in the appendix for reference.)
2. **We build the bot for them.** The operator fills in all bot details (display name, bio, tone, URLs, etc.) on the client's behalf. We may request information from the client out-of-band (email/call/short info form), but there is **no client-facing onboarding wizard** in the standard flow. The client's first login lands them on a finished/provisioning dashboard.
3. **Full-site rebrand to `twowaybot`.** This renames the **entire public website** — logo, nav, page titles, meta/OG tags, marketing copy, and the SMS opt-in/compliance copy — not just the texting line. Everything brand-referencing reads from a single constant (`PLATFORM_BRAND`) so future renames are one-line. (Display capitalization e.g. "TwoWayBot" vs "twowaybot" — minor, confirm at build.) Domain change (e.g. zar.bot → twowaybot.*) is infra/DNS, noted but out of code scope.
4. **Keep BOTH account types, identical pathway** — `performer` (creators) and `business` (SMB) apply the **same way**; `account_type` just *clarifies* which they are. One apply form, one review queue, one approve action, one build/provision flow for both. The only differences are where the code **already** branches on `account_type` (webhook path: `/twilio/webhook?slug=` vs `/smb/inbound?tenant=`; config source: `creator_configs` vs `smb_bot_config`). Implementation note: the provisioning pipeline currently only auto-runs for `performer`, so `provision_new_creator` must be made to run for **both** (it already passes `account_type` to the phone step) — see Phase 2.
5. **Standard tier only, provision immediately on approval.** No tier selection in the product right now. On approval, `provision_new_creator` runs immediately on the shared `twowaybot` Brand/Campaign. (Premium / per-client A2P remains future work in plan 09.)
6. **Remove ALL billing/pricing from the site completely.** Delete/hide pricing pages, plan pages, pricing CTAs, credits widgets, billing pages, and the Stripe self-serve checkout funnel from the public site **and the client dashboard**. Billing infra stays in the backend (untouched) but is **not surfaced anywhere** in the UI. No `/pricing`, `/plans`, `/billing`, "choose a plan," or credits display.
7. **Team invites unchanged** — an invite to join an existing team prompts the invitee to sign up (set password), which brings them to that team's dashboard. This is the existing `operator_invites` flow and is the model the post-approval client invite reuses.
8. **`/signup` repurposed to set-password (invite-token only).** The route stays but only works with a valid invite token (approval invite or team invite); there is no token-less public signup. Login, password reset, and Google **login** (existing users) stay.
9. **Public application requires no account** — `POST /api/access-request` is unauthenticated (with rate limit + invisible CAPTCHA) so a creator can apply before they have a login. Matches Community.com.
10. **Manual approval from Operator HQ** — Brij reviews/approves leads in the **operator service HQ dashboard** (`operator/`, internal tool — same place as Notion sync / CRM), not the client-facing app. No auto-approval (volume 1–3/month; human touch is a feature).
11. **New-application alerts: Notion row + email** — every submission writes a Notion lead row **and** emails Brij (via Resend, same infra as welcome emails).

### Application form fields (researched — Community.com + 2026 B2B norms)
4–6 fields is the conversion/quality sweet spot; anchor on email; one open-text qualifier; **do not** ask audience/company size (infer it from their website/social link).
- **Full name** (single field)
- **Email**
- **I am a…** → `Creator / Performer` | `Business` (sets `account_type`)
- **Website or main social link** (vetting + lets us infer reach ourselves)
- **What are you looking to do?** (open text)
- **Phone** _(optional)_
- Hidden: referral source / UTM

---

## Implementation Plan

### Phase 0 — Decided (no code)
- [x] Pattern: **Option A**. Tier: **Standard only, immediate provisioning**.
- [x] Brand: **full-site rename to `twowaybot`** via a single `PLATFORM_BRAND` constant (operator config + frontend constant). Display capitalization confirmed at build.
- [x] Keep **both** `performer` and `business` account types.
- [x] Application fields (5 required + 1 optional): **full name**, **email**, **Creator/Performer vs Business**, **website or social link**, **what are you looking to do** (open text), **phone** (optional). Hidden: referral/UTM.
- [x] Review queue lives in **Operator HQ** (`operator/` internal dashboard). Alerts: **Notion row + email**.

### Phase 1 — Backend: lead capture (`access_requests`)
**Files:** `operator/app/db.py` (migration), `operator/app/routes/api.py` (new endpoint), `operator/app/notion_crm.py` (lead push), `operator/app/alert_writer.py` (ops alert hook — already in flight).

- [ ] Migration: new table
  ```sql
  CREATE TABLE IF NOT EXISTS access_requests (
      id            BIGSERIAL PRIMARY KEY,
      name          TEXT NOT NULL,
      email         TEXT NOT NULL,
      account_type  TEXT NOT NULL DEFAULT 'performer',  -- performer | business
      link          TEXT NOT NULL DEFAULT '',   -- website or main social link
      goal          TEXT NOT NULL DEFAULT '',   -- "what are you looking to do"
      phone         TEXT NOT NULL DEFAULT '',   -- optional
      source        TEXT NOT NULL DEFAULT '',   -- referral / utm (hidden)
      status        TEXT NOT NULL DEFAULT 'new', -- new|contacted|approved|rejected
      operator_user_id BIGINT REFERENCES operator_users(id) ON DELETE SET NULL,
      reviewed_by   BIGINT REFERENCES operator_users(id) ON DELETE SET NULL,
      reviewed_at   TIMESTAMPTZ,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  CREATE INDEX IF NOT EXISTS idx_access_requests_status ON access_requests(status, created_at);
  ```
- [ ] `POST /api/access-request` (unauthenticated): validate, rate-limit by IP, invisible CAPTCHA (closes known AUTH-62 gap), insert row (`status='new'`). Then **both**: (a) push a Notion **lead** row (reuse `notion_crm`, new "Leads" DB/view or a Status=lead), and (b) **email Brij** via Resend (same infra as welcome emails). Returns `{ success: true }`. Never creates `operator_users`, `bot_configs`, slug, credits, or provisioning.
- [ ] `GET /api/admin/access-requests` (Operator HQ / super-admin): list/filter leads for the review queue.

### Phase 2 — Backend: operator-built bot + approval action
**Files:** `operator/app/routes/api.py`, `operator/app/routes/auth.py`, `operator/app/provisioning/__init__.py`.

- [ ] **Extract bot creation into a reusable function** `create_bot_for_user(user_id, form)` from the current `api_onboarding_submit` body (bot_configs upsert, slug + account_type on user, trial credits, team owner, Notion customer, start `provision_new_creator`). The admin approve action calls this directly. Keep it internal/server-side.
- [ ] **Remove the public self-serve bot path.** `POST /api/onboarding/submit` is either deleted or locked to super-admin only (no public access). There is no client-facing wizard in the standard flow, so the public submit endpoint should not be reachable by normal users. (Since the operator builds the bot, we don't need a per-user `approval_status` gate — the absence of a slug already means "no bot," and only the admin approve action creates one.)
- [ ] **Remove public signup.** `POST /api/auth/signup` and `GET /api/auth/google?signup=true` are disabled / return an "apply for access" message. Account creation happens **only** via invite (the approval invite below, or the existing team `operator_invites` flow). Login, password reset, and Google **login** (for existing users) stay.
- [ ] **Unify provisioning for both account types.** `create_bot_for_user` runs the **same** flow regardless of `account_type`; it always starts `provision_new_creator`. Today provisioning is gated to `performer` only — remove that gate so `business` provisions too. The pipeline already passes `account_type` to `phone.buy_and_configure` (which picks the right webhook), and config writes go to `creator_configs` vs `smb_bot_config` based on type. Confirm each pipeline step (config_writer, ingestion, notifications) has a sensible business branch or is type-agnostic; fill any gap so the single flow works end-to-end for both.
- [ ] **Admin approve endpoint** `POST /api/admin/access-requests/<id>/approve` (Operator HQ / super-admin). Body carries the operator-filled bot details (display name, slug, bio, tone, URLs, account_type, etc.):
  1. Create `operator_users` for the lead email (active, no password yet).
  2. Call `create_bot_for_user(user_id, form)` → bot_configs + slug + credits + team owner + Notion customer + **immediate provisioning** (same `provision_new_creator` for performer and business).
  3. Issue a set-password invite token (reuse `password_reset_tokens`) and email the client an invite link.
  4. Mark `access_requests.status='approved'`, set `operator_user_id`, `reviewed_by`, `reviewed_at`.
  5. Idempotent: re-approving the same lead does not double-create the user or re-buy a number (`provision`/`phone` are already idempotent).
- [ ] **Reject endpoint** `POST /api/admin/access-requests/<id>/reject` — set status, optional templated email.

### Phase 3 — Frontend: re-point the funnel to "Apply" + strip pricing
**Files:** `src/lib/create-bot.ts`, new `src/pages/Apply.tsx` (+ route), `EarlyAccessDialog.tsx`, marketing CTA components, `App.tsx`.

- [ ] **New `/apply` page** (or repurpose `EarlyAccessDialog`): the 5-field application form (full name, email, Creator/Business, website/social link, what-you-want; phone optional) → `POST /api/access-request` → success screen ("Thanks — we'll reach out within 24h"). No login required, no account created. Both `/performers` and `/business` pages route here (account_type prefilled from which page they came from).
- [ ] **Rewire `useCreateBot()`** — the single chokepoint. Replace its logic: anonymous / un-onboarded users → `/apply`. Users who already have a slug → their dashboard (unchanged). It no longer routes anyone to `/signup` or `/onboarding`. This one change flips nearly every "Create My Bot" CTA to "Apply" at once.
- [ ] **Update CTA copy** — "Create My Bot" → "Apply for access" / "Request a demo" across `Navbar`, `CTA`, `Performers`, `Business`, `HowItWorks`. The `CTA.tsx` section already says "Early access" — align the button.
- [ ] **Remove ALL pricing/billing from the UI** (decision 6): delete/hide routes `/pricing`, `/plans`, and `/:slug/billing`; remove `Pricing`, `Plans`, `PricingGrid`, `components/Pricing.tsx`, the Billing page, and `CreditsWidget` / credits chips from nav, pages, and the dashboard; remove all "Get Started / Choose plan / Start free trial" CTAs (`Faq.tsx`, `PricingGrid`, `Plans`); retire the `start-checkout` funnel. (Backend billing untouched, just not surfaced anywhere.)
- [ ] **Keep untouched:** `/login`, password reset, `SlugGuard`, all `/:slug/*` dashboard routes (except billing), `ProvisioningBanner`, team-invite acceptance.

### Phase 4 — Frontend: invite → set password → dashboard
**Files:** `src/pages/Signup.tsx` (→ invite/set-password mode), `src/pages/Dashboard.tsx`, `App.tsx`.

- [ ] **Invited-client entry:** the approval invite link opens a set-password screen (reuse `ResetPassword`/`ForgotPassword` token pattern). On submit → log in → redirect to `/{slug}/dashboard`. The client never sees an onboarding wizard.
- [ ] **Team invites unchanged:** existing `operator_invites` flow — invitee is prompted to sign up (set password), then lands on that team's dashboard. Verify this still works end-to-end after public signup is removed (the invite path must remain the *only* way to create an account).
- [ ] **Remove the public onboarding wizard from the funnel:** `/onboarding` route + `src/pages/Onboarding.tsx` are no longer reachable by normal users (operator builds the bot). Keep the code parked or delete; remove `RequireOnboarding`'s redirect-to-`/onboarding` and the Dashboard onboarding-guard redirect, since an approved client always has a slug by the time they log in.
- [ ] **First-login dashboard:** client lands on the dashboard with `ProvisioningBanner` showing "Setting up…" → "Live + your number" (provisioning already kicked off at approval).

### Phase 5 — Operator HQ review queue
**Files:** new route + template in the **operator service** (`operator/app/routes/` + operator HQ templates) — the internal dashboard, NOT the Lovable app and NOT `app/admin/` (that's the main-app dashboard).

- [ ] Lead list page in Operator HQ: name, email, account_type, link, goal, phone, status, created_at.
- [ ] Per-lead **build & approve** form: operator fills the bot details (display name, slug, bio, tone, URLs) → submits → calls the approve endpoint → creates user, builds bot, provisions, emails invite. Plus reject / mark-contacted actions.
- [ ] Link each lead to its resulting `operator_users` + `bot_configs.provisioning_status` once approved.

### Phase 6 — Full-site rebrand to `twowaybot`
**Files:** frontend brand constant + every place the old brand appears; operator `PLATFORM_BRAND`.

- [ ] Introduce `PLATFORM_BRAND` constant (frontend + operator) and route ALL visible brand text through it.
- [ ] Replace logo, nav wordmark, page `<title>`s, meta/OG tags, favicon, footer, and marketing copy with `twowaybot`.
- [ ] Update SMS opt-in / compliance copy + A2P campaign sample messages to reference `twowaybot` (ties to plan 09 Phase 0).
- [ ] Confirm display capitalization (e.g. "TwoWayBot").
- [ ] Domain/DNS change (zar.bot → twowaybot.*) and `VITE_API_BASE` update — infra task, tracked separately, noted here for completeness.

### Phase 7 — Marketing copy & compliance alignment
- [ ] Remove "instant" / "in minutes" / "self-serve" language everywhere; replace with "we build it for you" / managed-service framing (matches `matthew_berry_ai_proposal.md` tone).
- [ ] No pricing/billing claims anywhere public (removed this milestone).

---

## Files Changed Summary

| File | Change |
|---|---|
| `operator/app/db.py` | Migration: `access_requests` table |
| `operator/app/routes/api.py` | `POST /api/access-request`; admin list/approve/reject; extract `create_bot_for_user`; remove/lock public `onboarding/submit` |
| `operator/app/routes/auth.py` | Remove public signup; issue set-password invite on approval; keep login + team-invite flow |
| `operator/app/provisioning/__init__.py` | Triggered from admin approve (Standard, immediate); pipeline internals unchanged |
| `operator/app/notion_crm.py` | Push **leads** (pre-bot) in addition to customers |
| `operator/app/alert_writer.py` | Ops alert on new access request |
| `operator/app/config.py` (or similar) | `PLATFORM_BRAND = "twowaybot"` constant |
| `operator/app/routes/` + operator HQ templates _(new)_ | Operator HQ lead review queue (list + build/approve/reject) |
| `lovable-frontend/src/lib/create-bot.ts` | Route un-onboarded users to `/apply`; never to `/signup`/`/onboarding` |
| `lovable-frontend/src/pages/Apply.tsx` _(new)_ + `App.tsx` | Application form + route; remove `/pricing`, `/plans`, `/billing`, `/onboarding` from funnel |
| `lovable-frontend/src/components/{Navbar,CTA}.tsx`, `pages/{Performers,Business,HowItWorks,Faq}.tsx` | CTA copy → "Apply"; remove pricing links; rebrand |
| `lovable-frontend/src/components/{Pricing,PricingGrid,CreditsWidget}.tsx`, `pages/{Pricing,Plans,Billing}.tsx`, `lib/start-checkout.ts` | Removed/retired from site (pricing + billing + credits) |
| `lovable-frontend/src/pages/{Signup,Dashboard}.tsx`, `RequireOnboarding` | Signup → invite/set-password only; drop onboarding-redirect guards; remove credits/billing from dashboard |
| `lovable-frontend/src/lib/brand.ts` _(new)_ + logo/title/meta/favicon | `PLATFORM_BRAND` constant; full-site rebrand to twowaybot |
| `docs/engineering/lovable_frontend_wiring.md` | Document `/api/access-request`, approve/reject, invite flow, removed pricing/signup, rebrand |

---

## Testing Plan

### Backend (pytest)
- `POST /api/access-request`: valid insert; missing fields → 400; rate-limit/CAPTCHA enforced; **never** creates `operator_users`/`bot_configs`/slug/credits/provisioning.
- Public `POST /api/auth/signup` and `?signup=true` OAuth → rejected.
- Public `POST /api/onboarding/submit` → unreachable / 403 for normal users.
- `create_bot_for_user`: creates bot_configs + slug + credits + team owner + Notion customer + starts provisioning (regression of prior submit behavior).
- Approve action: creates user, builds bot, starts provisioning immediately, issues invite token, marks lead approved; idempotent on double-approve (no dupe user, no re-buy).

### Frontend
- Every "Create My Bot"/CTA now lands anonymous users on `/apply`.
- `/pricing`, `/plans`, `/onboarding` no longer reachable; no pricing CTAs anywhere public.
- `/login` + `/:slug/dashboard` unchanged for existing customers.
- Approved client: invite link → set password → dashboard + provisioning banner (no wizard).
- Team invite: invitee prompted to sign up → lands on team dashboard (still works as only account-creation path besides approval).

### Manual / smoke
- Submit a real application → Notion lead + Brij alert fire; **nothing** provisioned, **no** account created.
- Approve it (operator fills bot details) → bot provisions immediately (Standard) → client gets invite → sets password → dashboard shows number.
- Confirm existing Zarna login/dashboard is byte-for-byte unchanged.

---

## Explicitly Out of Scope (this milestone)
- Per-customer A2P Brand/Campaign automation (premium tier filing) — tracked in plan 09; this plan only routes the *decision* to the right provisioning path.
- Re-enabling any true instant self-serve tier (possible later for a low-end Standard plan).
- Conversational/AI intake agent (2026 "concierge layer") — a short form is enough at current volume; revisit when lead volume justifies it.
- Calendly/scheduling integration — optional add-on to the success screen later.

---

## Open Questions (remaining)
1. **Display capitalization** of the brand: `twowaybot` vs `TwoWayBot` vs `Two Way Bot`.
2. Domain/DNS cutover timing (zar.bot → twowaybot.*) — infra, not code.
3. Verify the business branch of each provisioning step (config_writer / ingestion / notifications) is sound when we unify the flow — a build-time check, not a blocker.

_Resolved:_ Option A; **full-site rebrand to twowaybot**; **both performer + business apply the same way** (account_type just clarifies; one unified build/provision flow); **operator builds the bot** (no client wizard); **Standard tier, immediate provisioning**; **all pricing + billing removed from UI**; **Operator-HQ review queue**; **Notion + email alerts**; **`/signup` = invite-token set-password only**; **no Calendly**; **5-field apply form** (name, email, creator/business, website/social, goal; phone optional).

---

## Appendix — Alternative patterns considered
- **Option A (lead-first, account-later):** public form → `access_requests` → approve → invite → signup → onboarding. Cleanest separation, but more frontend states and a second account-creation step. Good if we want zero "pending account" rows.
- **Option C (Notion/Typeform + manual scripts):** disable public signup, collect leads in Typeform, run provisioning scripts manually per approved client. Fastest to ship, least productized — viable as a stopgap before Phase 1 lands.
