# Plan: Self-Serve End-to-End Bot Building (Real Twilio + Multi-Tenant Performer Routing)
_Created: May 29, 2026_
_Status: Architecture signed off (mixed-housing on one engine). Ready for implementation, Phase 0 → 7._

---

## What This Is

The Twilio A2P 10DLC campaign is now **approved**. This unlocks the final missing link in the self-serve product: programmatically buying real phone numbers and attaching them to our approved messaging service.

The goal: a user signs up on the site, completes the onboarding wizard, and gets a **live, working SMS bot on a real dedicated number — with zero manual operator steps and zero new deployments**.

Almost the entire pipeline already exists. This plan closes the two gaps that stand between "demo with fake numbers / one deploy per client" and "true self-serve."

---

## Current State (what's already built — do NOT rebuild)

| Capability | Status | Location |
|---|---|---|
| Email/password + Google signup | ✅ Live | `operator/app/routes/auth.py` |
| 4-step onboarding wizard | ✅ Live | `lovable-frontend/src/pages/Onboarding.tsx` |
| `POST /api/onboarding/submit` → seeds `bot_configs`, slug, trial credits, team owner | ✅ Live | `operator/app/routes/api.py` (~L4847–5037) |
| Provisioning orchestrator (background thread, idempotent, status-tracked) | ✅ Built | `operator/app/provisioning/__init__.py` |
| Personality generation (Gemini → `creator_configs`) | ✅ Built | `operator/app/provisioning/config_writer.py` |
| RAG ingestion (website scrape → pgvector `creator_embeddings`) | ✅ Built | `operator/app/provisioning/ingestion.py` |
| Welcome email (Resend) | ✅ Built | `operator/app/provisioning/notifications.py` |
| Provisioning status polling + "bot is live" + phone display | ✅ Live | `Onboarding.tsx`, `ProvisioningBanner.tsx`, `Dashboard.tsx` |
| Twilio outbound + inbound webhook + signature validation + STOP | ✅ Live | `app/messaging/twilio_adapter.py`, `main.py` (`/twilio/webhook`) |
| Per-slug brain (personality from DB, RAG from `PgRetriever(slug)`) | ✅ Works | `app/brain/handler.py` (`create_brain`, L339–402) |
| **SMB** multi-tenant routing by `To` number (one process, many tenants) | ✅ Live | `app/smb/tenants.py` (`TenantRegistry`) |
| Billing / Stripe / trial credits | ✅ Live | `operator/app/routes/billing*` |

---

## The Two Gaps

### Gap 1 — Phone provisioning is stubbed
`operator/app/provisioning/phone.py::_buy_real_number()` raises `NotImplementedError`. Stub mode returns deterministic fake `+1555XXXXXXX` numbers. The real SDK calls are spelled out in comments but not wired.

### Gap 2 — Performers are not routed by phone number
`main.py` creates **one global brain** at import (`brain = create_brain()`), so `/twilio/webhook` always replies as the process's `CREATOR_SLUG`. SMB already solved this with `TenantRegistry.get_by_to_number()`. Performers need the equivalent: **one process serving all creators, selecting the brain per inbound `To` number.** Without this, every self-serve signup would require a new Railway deployment — which defeats the point.

---

## Decisions (all locked)

1. **Housing model:** ✅ **One shared engine, mixed housing ("managed complex").** A single creator-agnostic codebase powers two deployment shapes:
   - **Apartment building (multi-tenant)** — one process serves many self-serve creators, routed by `To` number (mirror SMB's `TenantRegistry`). The default for the long tail. New creators fit natively (personality in `creator_configs`, RAG via `PgRetriever`/pgvector).
   - **Private house (dedicated instance)** — the *same* codebase deployed solo for a premium/marquee creator (Zarna today; Kevin-tier later). Full isolation, guaranteed capacity, voice preserved in their own space — sold as a premium/Enterprise tier. Not for raw speed (the LLM call dominates latency identically either way) but for isolation + capacity + the prestige/SLA story.
   - **Both shapes already exist in the codebase today** (Zarna = house, SMB = building), so this is formalizing a property the system already has, not inventing one.
   - **Zarna stays in her own house for now** (live, revenue-critical; her file-based `EmbeddingRetriever` RAG must move to pgvector before she could join a building). Migrate later or keep dedicated — no rework either way.
   - **Required principle — keep the engine creator-agnostic.** All creator identity lives in config + data + DB, never hardcoded. A "de-Zarna-ify" cleanup is in scope: move the remaining Zarna-specific bits out of engine code (hardcoded join-confirmation copy gated on slug in `app/live_shows/join_confirmations.py`; the Zarna-corpus default fallback in `app/config.py`; Zarna-flavored config fields) so any creator gets identical capability purely by filling in their own config. This is what makes "Kevin's voice in his own space, same tech" true.
   - Rationale: the self-serve vision requires the building regardless; auto-deploy-per-creator for *everyone* would be throwaway work and 10–30× infra cost at scale. Mixed housing on one engine is exactly what SlickText/Community/Attentive do (shared platform + DB, dedicated number per customer, dedicated infra reserved for enterprise).

2. **A2P / brand model:** ✅ **Single shared Messaging Service + our one approved campaign.** All customer numbers attach to it for launch. Caveat: shared T-Mobile daily throughput cap across all customers; we are the responsible party for content. Per-customer brand/campaign (Twilio ISV secondary brands) is a later milestone, triggered by throughput pressure.

3. **Channel:** ✅ **Twilio-only** for all new self-serve creators. SlickText stays Zarna's dedicated path **for now**; the design must not preclude migrating her later.

4. **Webhook tenant resolution:** ✅ **Explicit slug in the webhook URL** set at purchase time: `/twilio/webhook?slug=<slug>` (matches SMB's `?tenant=<slug>`), with a `To`→slug DB lookup as fallback/source-of-truth.

---

## Implementation Plan

### Phase 0 — A2P / Twilio account prerequisites (no code; verify + record)
- [ ] Confirm the approved **Messaging Service SID** and set `TWILIO_MESSAGING_SERVICE_SID` in the main-app + operator Railway envs.
- [ ] Confirm the campaign use-case, sample messages, and opt-in language on file match what the product actually sends (welcome + AI replies). If the welcome/compliance copy from plan 07 differs, update the campaign samples.
- [ ] Confirm STOP/HELP auto-responses are configured at the Messaging Service / Advanced Opt-Out level (so carrier-mandated replies fire regardless of app logic).
- [ ] Decide and document the **number pool strategy** (US local, area-code preference, fallback to toll-free if local unavailable).
- [ ] Record A2P throughput limits for the campaign so we can monitor against them.

### Phase 1 — Real phone provisioning
**File:** `operator/app/provisioning/phone.py`

Implement `_buy_real_number(slug)`:
1. Search available US local SMS-capable numbers (`client.available_phone_numbers("US").local.list(sms_enabled=True, limit=N)`), with optional area-code preference; fall back gracefully if the pool is empty.
2. Purchase via `client.incoming_phone_numbers.create(phone_number=..., sms_url=f"{WEBHOOK_BASE}/twilio/webhook?slug={slug}", sms_method="POST")`.
   - Performer → `/twilio/webhook?slug=<slug>`; Business → keep existing `/smb/inbound?tenant=<slug>`. Pick the path from `account_type`.
3. Attach to the A2P messaging service: `client.messaging.v1.services(msg_svc_sid).phone_numbers.create(phone_number_sid=purchased.sid)`.
4. Persist: `_save_phone_to_user(slug, purchased.phone_number)` (already exists) **and** write to the new phone→slug registry (Phase 2).
5. Idempotency: `_get_existing_phone` already short-circuits; also guard against double-purchase on retry.
6. Wrap each Twilio call with clear error messages so failures land in `bot_configs.error_message` (orchestrator already catches and stores).

**Env/config:** `PROVISIONING_PHONE_MODE=real`, `TWILIO_WEBHOOK_BASE`, `TWILIO_MESSAGING_SERVICE_SID`, plus area-code preference var. Keep `stub` mode fully working for tests/local.

**Tests:** mock the Twilio client — assert search→buy→attach→save sequence, webhook URL contains the slug, idempotent retry doesn't re-buy, empty-pool path raises a clean error.

### Phase 2 — Performer phone → slug registry (DB source of truth)
**Files:** `app/storage/postgres.py` (migration), new `app/performer/registry.py` (mirror of `app/smb/tenants.py`)

- [ ] Add a lookup table or reuse existing data. Source of truth options:
  - Reuse `bot_configs` + `operator_users.phone_number` (already populated by provisioning), OR
  - A dedicated `creator_numbers(phone_number PK, creator_slug, status, created_at)` table for a clean, indexed hot-path lookup. **Recommended** (decouples the main app from operator-user internals; one row per number).
- [ ] `PerformerRegistry.get_slug_by_to_number(to_number)` — memoized with a short TTL or refreshed on cache miss (new numbers appear at runtime, unlike SMB's startup-loaded JSON). On miss, hit the DB; cache the result.
- [ ] This is the **fallback / validation** path even though the webhook carries `?slug=`; it also lets us reject numbers that don't belong to us.

### Phase 3 — Per-slug brain cache + tenant-aware Twilio webhook
**Files:** `app/brain/handler.py` (or a small `app/brain/registry.py`), `main.py`

- [ ] `get_brain(slug)` — memoized factory: `create_brain(slug)` once per slug, cached process-wide (LRU/dict). Each brain already loads personality from `creator_configs` and uses `PgRetriever(slug)`. Keep the existing global `brain` for Zarna/SlickText backward-compat.
- [ ] In `/twilio/webhook` (`main.py` ~L728): resolve the tenant slug from `request.args.get("slug")`, validated against the Phase 2 registry by `To` number. If absent/invalid, fall back to `To`→slug lookup; if still unknown, log + 204 (don't reply as the wrong creator).
- [ ] Replace the hardcoded `brain` reference in `_process_twilio_message` with `get_brain(resolved_slug)`. Ensure `PostgresStorage(creator_slug=resolved_slug)` so fan data is stamped to the right tenant.
- [ ] The existing **SMB firewall** (drops messages addressed to SMB numbers) stays; add the symmetric guard so a performer number never falls through to the global Zarna brain.
- [ ] Per-slug rate limiting, dedup, and quiz/blast context all key off the resolved slug.

### Phase 4 — First-text experience & compliance (depends on plan 07)
Plan 07 (`07_performer_onboarding_and_bot_parity.md`) already specifies: `performer_subscribers` table, welcome message, signup question, contact-card vCard, and the A2P compliance line on first message. **Ship plan 07 as part of this** — it's the "what a fan sees when they first text a self-serve bot" layer.
- [ ] Ensure the compliance line (`"Msg & data rates may apply. Reply STOP to opt out."`) fires on first message for self-serve creators.
- [ ] Scope opt-out (`broadcast_optouts`) per `creator_slug` so a STOP to one creator doesn't suppress another (verify current behavior; fix if global).

### Phase 5 — Credit gating for self-serve AI replies
**Files:** `main.py` (`_process_twilio_message`), `app/brain/` credit hooks
- [ ] Performer AI auto-replies must consume + be gated by the resolved slug's credits (documented gap: performer replies may not currently be credit-gated on the main app). Without this, self-serve creators get unlimited free AI. Reuse `_consume_message_credits` and the `BILLING_HARD_GATE` check that the Twilio path already references — confirm they're keyed per slug.

### Phase 6 — Frontend polish (mostly done)
**Files:** `lovable-frontend/src/pages/Onboarding.tsx`, `ProvisioningBanner.tsx`, `Dashboard.tsx`
- [ ] Provisioning success already displays the real number once `phone.py` returns it — verify end-to-end with a real purchase.
- [ ] Add a "Text your bot to test it" nudge on the live screen (send to the new number).
- [ ] Wire the file-upload step (currently filenames-only, not sent to API) into ingestion **OR** explicitly defer it (note in copy that website scrape is the V1 training source).
- [ ] Surface provisioning failures (e.g. number pool empty) with a friendly retry (`POST /api/provisioning/retry` already exists).

### Phase 7 — Operator observability & guardrails
- [ ] Admin view of all provisioned numbers (slug, number, A2P status, created_at) — likely a new `app/admin/<tab>.py` submodule per file-organisation rule.
- [ ] Alert on provisioning failures (the `alert_writer.py` / cost-logger work in flight can hook here).
- [ ] Number lifecycle: what happens on cancellation/downgrade — release the number back to Twilio (`incoming_phone_numbers(sid).delete()`) to stop paying for it. Define the policy.
- [ ] Monitor A2P throughput against the campaign cap; alert before we hit it (drives the Phase-(2B) per-brand decision).

---

## Files Changed Summary

| File | Change |
|---|---|
| `operator/app/provisioning/phone.py` | Implement `_buy_real_number` (search → buy → attach to messaging service → set slug-bearing webhook → persist) |
| `app/storage/postgres.py` | Migration: `creator_numbers(phone_number, creator_slug, ...)` registry table |
| `app/performer/registry.py` _(new)_ | `PerformerRegistry.get_slug_by_to_number` (DB-backed, cached, runtime-refreshable) |
| `app/brain/handler.py` / `app/brain/registry.py` _(new)_ | `get_brain(slug)` memoized per-slug brain cache |
| `main.py` | Tenant-aware `/twilio/webhook`: resolve slug, `get_brain(slug)`, per-slug storage + credit gate; performer-number firewall |
| `operator/app/provisioning/__init__.py` | Pass `account_type` to phone step so webhook path differs performer vs business |
| `app/performer/onboarding.py` _(plan 07)_ | First-text welcome + compliance + subscriber row + vCard |
| `lovable-frontend/.../Onboarding.tsx` | "Text to test" nudge; failure UX; (optional) file-upload wiring |
| `docs/engineering/lovable_frontend_wiring.md` | Document any new fields / provisioning states |

---

## Testing Plan

### Unit
- `phone.py` real-mode: mocked Twilio — search/buy/attach/save order, slug in webhook URL, idempotent retry, empty-pool error, business vs performer webhook path.
- `PerformerRegistry`: `To`→slug hit/miss, cache refresh on new number, unknown number returns None.
- `get_brain`: returns distinct brains per slug, caches per slug, never leaks Zarna brain for a non-zarna number.

### Integration
- Full provision in a Twilio **test/subaccount**: submit onboarding → real number bought → attached to messaging service → webhook set → row in `creator_numbers`.
- Inbound to creator A's number replies as A; inbound to creator B's number replies as B — in the **same process**.
- First text → welcome + compliance line + (optional) vCard; STOP suppresses only that creator.
- Credit gate: drain a creator's credits → AI replies stop for that creator only.

### Compliance / safety
- STOP/HELP carrier auto-responses fire (Messaging Service level).
- No reply ever sent from the wrong creator's number/voice.
- Opt-out scoped per creator_slug.
- Numbers released on cancellation (no orphaned paid numbers).

### Manual / smoke
- Buy a real number on a low-volume test, text it from a personal phone, confirm voice + links + compliance.
- Cancel → confirm number released and webhook traffic stops.

---

## Explicitly Out of Scope (for this milestone)
- Per-customer A2P brand/campaign registration (Twilio ISV secondary brands) — revisit when shared-campaign throughput becomes a constraint.
- Number porting / vanity numbers / toll-free vetting.
- Auto-deploy-per-tenant (explicitly rejected in favor of single-process routing).
- Rich content ingestion beyond website scrape (YouTube/podcast/specials) — managed-tier concern, not blocking self-serve V1.
- SlickText for self-serve creators.

---

## Open Questions
1. Single shared A2P campaign for all self-serve customers at launch — confirmed acceptable for compliance + throughput at expected early volume?
2. Number pool: US local only, or toll-free fallback when local is unavailable in the desired area code?
3. On plan cancellation/downgrade — release the number immediately, or hold for a grace period?
4. Does the approved campaign's registered sample copy already cover AI-generated conversational replies (not just the welcome)? If not, update samples before going live.
5. Should Zarna eventually migrate onto this multi-tenant Twilio path, or stay on her dedicated SlickText deployment indefinitely?
