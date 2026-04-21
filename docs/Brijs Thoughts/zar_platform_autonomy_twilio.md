# Zar: Full Autonomy on the Web (Twilio, No SlickText)

*Strategic north star, architecture, and execution notes — consolidated so engineering and product stay aligned.*

---

## 1. North star

**Zar is the product.** A single web surface (marketing + authenticated app) where a customer can:

1. **Onboard with minimal manual work** — supply name, business or performer context, content, and goals; the system provisions their bot and tenant configuration over time (full autonomy is the end state, not day-one requirement).
2. **Operate everything in one place** — subscribers, conversations, blasts, segments, tracked links, and quality signals (reply rate, silence, opt-outs, conversions) in a coherent dashboard comparable to what operators expect from a modern SMS platform, but differentiated by **AI, intent, and closed-loop improvement**.
3. **Run on Twilio only** — no SlickText. Twilio provides **numbers, messaging APIs, webhooks, and carrier-side plumbing**; Zar owns **data, UX, AI, analytics, and compliance UX**.

**Elevator line:** CPaaS (Twilio) under the hood; Zar as the full application and brand on top.

---

## 2. Why this direction (decision record)

### 2.1 Why cut SlickText

- **Underused features** — pricing and complexity pay for an ESP-shaped product that does not match how Zar is used.
- **Limits** — product and API constraints cap differentiation (custom flows, AI-first inbox, unified analytics tied to your data model).
- **Strategic ownership** — If Zar is the platform, the **operator console** must eventually live in Zar, not in a third-party marketer UI.

### 2.2 Why Twilio (and what Twilio is *not*)

- Twilio provides **reliable SMS/MMS transport**, phone numbers, webhooks, and the regulatory path (e.g. 10DLC registration) in a way that fits **programmable** products.
- Twilio does **not** ship a SlickText-class **fan CRM + blast composer + marketer dashboards** out of the box. **Zar must build that layer.** That is intentional: the dashboard is the product surface where AI and analytics differentiate.

### 2.3 Alignment with existing build

- **SMB** already uses Twilio-oriented flows, tenant configs, engagement scoring, quality digest, and an operator portal pattern.
- **Creator / Zarna** path is moving to Twilio for inbound/outbound to escape SlickText limits.
- **Scripts and env automation** (e.g. Railway variable provisioning) support the thesis: **data + config in repo; infrastructure applied via API** where safe.

---

## 3. Definition of “full autonomy” (staged)

**End goal:** A user enters structured inputs (identity, links, tone, content sources); within a bounded time the system **creates or updates** their bot, phone routing, prompts, retrieval corpus where applicable, and dashboard access — with **no engineer in the loop** for routine onboarding.

**Realistic staging:**

| Stage | Autonomy | Notes |
|-------|----------|--------|
| A | Config + env from scripts / admin | Today-adjacent: JSON + `onboard_client.py` + deploy. |
| B | Self-serve forms → server job → Twilio + DB + dashboard invite | First “productized” autonomy; secrets only server-side. |
| C | Content ingestion (uploads, URLs) → chunking + embeddings pipeline | Higher effort; quality gates needed. |
| D | Continuous improvement visible in UI | Quality digest, winners, segments — already partially built for SMB/creator backends. |

Treat **full autonomy** as **C + B + D** with clear SLAs (e.g. “bot live within X minutes for SMB template”) rather than a single big bang.

---

## 4. Product surface: what lives on the website

### 4.1 Public marketing (existing direction)

- Light, bright, **Notion / Squarespace–inspired** marketing site (Lovable or similar for rapid UI iteration).
- Dual ICP: **performer / manager** vs **business owner** — **whole-page mode switch**, not a single headline swap.
- Sections: hero, proof (Zarna case study with **honest / placeholder** metrics where needed), features, pricing tiers, FAQ, privacy, terms.
- Primary CTA evolves from waitlist → **sign up / enter app** when the authenticated product is ready.

### 4.2 Authenticated product (Zar app)

Single logged-in experience (subdomain or path such as `app.` or `/app`):

- **Subscribers** — list, status, opt-in source, tags/segments, import/export rules (with compliance).
- **Conversations** — thread view, search, optional human takeover, AI-suggested replies where appropriate.
- **Blasts** — compose, segment, schedule, delivery stats; link tracking and conversion-style events surfaced in the same UI.
- **Analytics** — reply rate, silence rate, opt-outs, click-through, blast performance, weekly quality summary (aligned with existing digest thinking).
- **Settings** — tenant slug, Twilio numbers, tracked links, voice/guardrails (creator), tone and copy (SMB), portal-style controls merged into one product-grade settings area.

**Principle:** Anything operators today do in **separate** tools (SlickText, fragmented portals) should **converge here** over time.

---

## 5. Technical architecture (high level)

### 5.1 Layers

1. **Edge / web** — Marketing site + SPA or SSR app for Zar; auth (session or token-based); strict CSP and HTTPS.
2. **API** — Tenant-scoped REST or GraphQL; **no Twilio secrets or Railway tokens in the browser.**
3. **Workers / cron** — Scoring, digests, blast dispatch, ingestion jobs (pattern already used in Railway crons).
4. **Data** — Postgres (or current store) with **hard tenant isolation** on every query.
5. **Twilio** — Inbound webhooks to Zar; outbound sends from Zar with idempotency and audit logs.

### 5.2 Repository and deploy shape (recommended)

- **Marketing** and **app** can share a monorepo or split repos; either works if boundaries are clear.
- Prefer **two deployables** (e.g. static marketing + API-backed app) so marketing experiments do not risk production messaging.

### 5.3 Identity and multi-tenancy

- One user can belong to one or more tenants later (agencies); **v1** can be **one login ↔ one tenant** to reduce scope.
- Role model: owner vs staff (defer until needed).

---

## 6. Twilio migration and de-SlickText checklist

Use this as a living checklist when cutting ZarnaBot (and any other traffic) over fully.

1. **Parallel readiness** — Twilio number(s), webhooks, signature validation, same business logic paths as current provider.
2. **Subscriber and consent data** — Export anything that only lives in SlickText; map fields into Zar’s canonical subscriber model.
3. **Cutover** — Short window with monitoring; avoid double webhooks; document rollback (revert DNS/webhook URL).
4. **Compliance** — STOP/HELP behavior, opt-in evidence, 10DLC / campaign registration as required for traffic type.
5. **Observability** — Structured logs (existing `[ZARNA]` / `[SMB]` style prefixes) and alerts on error rate and latency post-cutover.
6. **Decommission** — Cancel SlickText billing only after traffic and reporting have been stable on Twilio for an agreed period.

---

## 7. Compliance, trust, and risk (non-negotiables)

- **TCPA / consent** — UI and data model must record *how* someone opted in; blasts must respect opt-out and quiet hours where applicable.
- **AI disclosure** — Where legally or ethically required, clear labeling that automated replies may be used; configurable per tenant if needed.
- **Data minimization** — Export and deletion paths for GDPR-style requests as the product matures.
- **Security** — Rate limits on auth and webhooks; no secrets in client bundles; audit sensitive actions (exports, blast sends).

Existing compliance notes in the repo (e.g. `docs/sms_compliance_sections.md`) should be **referenced and extended** when the unified app ships customer-facing consent flows.

---

## 8. Relationship to current codebase (anchors)

These areas are already directionally aligned with this doc; future work **extends** them rather than replacing the vision wholesale.

- **SMB** — `app/smb/*`, tenant JSON, Twilio webhooks, engagement columns, quality digest script, operator portal patterns.
- **Creator** — `app/brain/*`, `CreatorConfig`, intent/selling pipeline, quality digest for main AI.
- **Onboarding automation** — `scripts/onboard_client.py` for Railway env provisioning from `creator_config/<slug>.json`.
- **Logging** — Service-prefixed formatter in `main.py` for filterable production logs.

**Gap to close for the north star:** a **first-class web app** with auth that exposes subscriber/conversation/blast/analytics UIs for *all* customer types, backed by the same primitives you already use internally.

---

## 9. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Building ESP parity slowly | Ruthless **MVP slice**: inbox + subscribers + one blast path + core metrics before advanced campaign features. |
| Security incident on unified app | Separate deployables, security review on auth and exports, least-privilege API keys. |
| Twilio / carrier friction | Budget time for registration; keep support runbooks. |
| Scope creep (“replace everything SlickText did”) | Maintain a **written parity backlog**; ship in vertical slices. |

---

## 10. What “done” looks like (outcome criteria)

Short list to revisit quarterly:

1. **No SlickText** in production paths for ZarnaBot (and any other in-scope numbers).
2. **Twilio-only** transport for inbound/outbound SMS in scope.
3. **Customers** log into **Zar** to see subscribers, conversations, blasts, and key metrics in one UI.
4. **Onboarding** moves from “engineer + scripts” toward “customer + server automation” with measurable time-to-live.
5. **Single brand story** — marketing and product feel like one company, not a landing page plus mystery tools.

---

## 11. Open decisions (fill in as you decide)

- **Domain strategy** — `zar.com` marketing vs `app.zar.com` product (example only).
- **First vertical for the unified app** — SMB-only first vs performer-only first vs dual mode from day one (dual mode costs more UX and QA).
- **Auth vendor** — Build minimal auth vs Clerk / Auth0 / similar (trade cost vs speed).
- **Timeline** — Target quarter for “no SlickText + Twilio-only ZarnaBot” vs target quarter for “first self-serve dashboard MVP.”

---

*Last updated: April 2026. Revise this doc when cutover milestones or product scope change.*
