# Plan: Matthew Berry Toll-Free Provisioning & Verification

_Created: June 3, 2026_
_Status: Planning. Nothing submitted yet. Awaiting business info from Fantasy Life (see "Information We Need")._

---

## What This Is

The provisioning + carrier-registration plan to take the "Matthew Berry AI Experience" live on SMS. It covers the messaging channel decision, the verification submission, the opt-in/compliance design, the technical wiring into the dedicated deploy, and the exact information required from Matthew / Fantasy Life before anything can be submitted.

**This is an internal execution plan, not the client-facing proposal** (that lives at `docs/business/matthew_berry_ai_proposal.md`).

---

## ✅ Confirmed by Twilio Support (live chat, Jun 3, 2026)

- **Brand/registration:** "For ISVs/platforms supporting multiple end businesses, the toll-free number should be registered under the **end business** (your client's legal entity and EIN), **not your own platform's brand**." → Register under **Fantasy Life**; ZarBot is the ISV.
- **High throughput:** State expected **volume, use case, and need for higher throughput** in the verification application (carriers may request more docs). **High-throughput toll-free (HTTF) requires engaging Twilio's Sales team** — it is not granted via the standard verification form, and carries an **additional monthly fee**.
  - Per Twilio's GA announcement, **HTTF burst throughput starts at 25 MPS** (negotiable higher via Sales). **25 MPS = ~90,000 msgs/hour**, which already covers Matthew's volume with huge headroom (a full ~40k blast clears in ~27 min; reactive spikes are trivial). No exotic tier needed.
- **Timeline:** Toll-free verification **typically 5-10 days**, varies with carrier backlog + application completeness.
- **Top rejection reasons to avoid:** (1) incomplete/inaccurate business info (legal name, EIN); (2) vague/non-compliant use-case description; (3) missing clear opt-in process or privacy policy; (4) use case not allowed by carriers (prohibited content / non-compliant marketing).
- **ISV account structure (exact order):**
  1. Set up **our** platform's **Primary Customer Profile** (business type: **ISV/Reseller**) in the **parent** account.
  2. Create a **subaccount per client** (end business).
  3. Create a **Secondary Customer Profile** for the client (their legal entity, EIN, etc.).
  4. Register the client's Brand / Trust Product (Toll-Free Verification) **under their subaccount**.
  5. Submit the TFV under the **client's Secondary Customer Profile, using the client's business info (NOT our ISV info)**. The toll-free number resides in the client's subaccount.
- **Opt-in proof:** acceptable evidence = screenshot of the web form / opt-in page, a description of the opt-in flow (keyword, checkbox, etc.), or the opt-in URL. **Single opt-in is sufficient** — double opt-in is not required unless the use case is high-risk / sensitive.
- **Privacy policy:** must **explicitly state that mobile opt-in data is not sold or shared**, and must be **accessible on the end business's (Fantasy Life's) domain** — not just ours. (So the consumer-facing policy needs to live on a Fantasy Life URL.)
- **AI-generated content:** **allowed**, but must comply with Twilio's Acceptable Use Policy + carrier guidelines. The **use-case description must mention AI-powered conversational messaging** and explain **how opt-in and content moderation are handled**.
- **Pre-verification behavior:** until the number is fully verified, **only low-volume conversational traffic is allowed — the TFN behaves like a regular US 10DLC**. Do not launch announcements/blasts until verification (and HTTF) are approved.
- **ISV Primary Customer Profile approval GATES everything:** typically a **few business days** (longer if extra docs needed). We **cannot register Fantasy Life's profile or submit the TFV until our ISV/Reseller Primary Customer Profile is approved**. → **Start this first, now, in parallel with gathering Fantasy Life's info.**
- **Existing list import:** ✅ allowed onto the new toll-free **with valid proof of prior consent per subscriber** (single opt-in generally sufficient).
- **MMS:** ✅ toll-free supports MMS, **covered by the same verification** as SMS (so contact-card / image sends work).
- **Subaccount portability:** moving a verified toll-free between subaccounts **does NOT carry the verification** — it must be re-verified. → **Provision directly in Fantasy Life's subaccount from the start.**
- **If rejected:** **7 days to resubmit with corrections and keep the queue position**; after 7 days you can still resubmit but lose your spot in line.
- **Support-plan upgrade (recommended de-risk for this launch):** the higher-tier paid support plan (~$5k/mo tier) includes **live phone + screen-share walkthroughs** — Twilio confirmed they'll guide us live through ISV Primary Customer Profile setup, subaccount + Secondary Customer Profile creation, and toll-free verification end-to-end, plus compliance guidance and **expedited troubleshooting** (not just ticket-based). Worth it to avoid a rejection/resubmit cycle on a marquee launch. Plans: https://www.twilio.com/en-us/support-plans

---

## Decision Summary (locked)

| Decision | Choice | Why |
|---|---|---|
| Hosting | **Dedicated deploy** (own Railway project, own DB, `CREATOR_SLUG`) | Marquee client; isolation, capacity, prestige. Mirrors the Zarna model. |
| Channel | **Twilio high-throughput toll-free (HTTF)** | Audience is tens of thousands. Toll-free handles it with headroom, provisions in weeks (not the 6-10 wks of a short code), avoids the 10DLC per-day cap, and costs a fraction of a short code. |
| Registration path | **Toll-Free Verification (TFV)**, single form | Toll-free does **not** use TCR brand + campaign registration. One verification form replaces the whole 10DLC brand→vetting→campaign sequence. |
| Registered business (verification) | **Fantasy Life** = the registered end business (legal name, address, **EIN**, `fantasylife.com`, contact). **ZarBot = the ISV** that submits + operates on their behalf. | **Carrier requirement, not a preference.** Toll-free verification must identify the end business the consumer recognizes, NOT the platform (Twilio reject codes 30472/30474). Registering under ZarBot would be rejected. |
| Brand shown to fans | **"Matthew Berry"** | Allowed: the public brand only needs to be "directly representative" of the registered entity (CTIA Pepsi→Mountain Dew principle). Fantasy Life is verifiably his company. (ZarBot→"Matthew Berry" would be the flagged mismatch.) |
| Short code | **Deferred to Phase 2** | Only if he wants a vanity "Text BERRY to #####" TV CTA or scales to hundreds of thousands. Not needed for current volume. |

> Note on terminology: the user asked for "campaign/brand." For **toll-free**, there is no separate TCR Brand + Campaign — the equivalent is the **Toll-Free Verification (TFV)** submission. This plan uses TFV throughout. (If we ever move him to 10DLC or short code, the TCR brand+campaign path applies instead — out of scope here.)

---

## Our Role as Provider — the SlickText Parallel

**We (ZarBot) are doing exactly what SlickText does for Zarna — nothing more, nothing barred.** This section exists because the natural question is "SlickText handled Zarna's toll-free, why can't we just register it under ourselves?" The answer: SlickText never registered Zarna's number under *SlickText the business* — it registered Zarna's business and submitted on her behalf. Same role we play here.

### Shared number vs. dedicated number (the core distinction)

| Model | Who is the registered business | Who rides it | Where the EIN burden sits | Example |
|---|---|---|---|---|
| **Shared short code** | The platform | Many brands as "campaigns/textwords" within it | Platform | (legacy short-code platforms) |
| **Dedicated number** (toll-free / 10DLC) | **The end business** | n/a — it's their own number | **End business** | **SlickText + Zarna; ZarBot + Matthew** |

A dedicated toll-free cannot be parked under a platform's umbrella registration — carriers require the **end business** the consumer recognizes to be named (Twilio reject code 30474 = "submitted the ISV/platform instead of the end business"). Sharing one toll-free/10DLC registration across unrelated brands is "snowshoeing" and is prohibited.

### The mapping

| SlickText + Zarna | ZarBot + Matthew |
|---|---|
| SlickText = provider that submits the TFV | **ZarBot = provider that submits the TFV** |
| Zarna's business = the registered identity on the form | **Fantasy Life = the registered identity** |
| SlickText gathered the info + did the paperwork | **ZarBot gathers the info + does the paperwork** |

### Why the Zarna setup *felt* like it needed no EIN

1. SlickText absorbed the friction (collected details during onboarding, submitted for her).
2. **Timing:** the mandatory **EIN** for toll-free is new as of **Feb 17, 2026**. If Zarna's toll-free was verified before that, it could pass with just business name + website + opt-in — no EIN. That's almost certainly why no EIN was required then.

**Conclusion:** the EIN requirement follows the *toll-free number*, not the platform. Setting Matthew up through SlickText today would require Fantasy Life's EIN just the same. We are not constrained relative to SlickText — we are the SlickText-equivalent provider, and Fantasy Life is the Zarna-equivalent registered business.

---

## Channel Sizing (reference)

Audience: **tens of thousands** (~3-5x Zarna's 10k).

- **Reactive Sunday spike:** ~20% of a 40k list texting in over the late-morning window ≈ 8,000 replies; clears in ~13 min even at 10 MPS. Non-issue.
- **Full blast (heaviest case):** 40k recipients → ~16-20k segments to T-Mobile. Toll-free has **no T-Mobile per-day cap** (unlike 10DLC), so this is fine once high-throughput is granted.
- HTTF throughput once verified: rivals short code (up to ~100 MPS). Ample.

---

## Prerequisites (our side, before submitting)

- [ ] **START FIRST (gates everything):** Submit our platform's **Primary Customer Profile (business type: ISV/Reseller)** in the parent account and get it approved (~few business days). Nothing downstream — Fantasy Life's profile, the TFV — can proceed until this is approved.
- [ ] **Provision the number directly in Fantasy Life's subaccount** (moving it later forces re-verification).
- [ ] Twilio account with a verified payment method for the dedicated deploy (his own subaccount recommended for clean billing/isolation).
- [ ] Decide subaccount structure: dedicated Twilio **subaccount** for Fantasy Life vs. main account (recommend subaccount).
- [ ] Confirm `TWILIO_WEBHOOK_BASE` for his deploy points at the main app's public URL.
- [ ] Privacy Policy + Terms URLs live and reachable (from Fantasy Life — see Information We Need).
- [ ] Opt-in flow built/described with a screenshot or hosted page (carriers require proof of consent).

---

## Submission Process (step by step — do NOT execute until info is gathered)

### Step 1 — Buy the toll-free number
Purchase a dedicated toll-free number (`833`/`844`/`855`/`866`/`877`/`888`) in Fantasy Life's Twilio (sub)account.

### Step 2 — Create a dedicated Messaging Service
Create a Messaging Service for Fantasy Life and add the toll-free number to its sender pool. Record the **Messaging Service SID** → becomes his deploy's `TWILIO_MESSAGING_SERVICE_SID`.

### Step 3 — Submit the Toll-Free Verification (TFV) + engage Sales for high throughput
Submit the TFV form (fields detailed in the next section) and **explicitly state expected volume, use case, and need for higher throughput** in the application. Per Twilio support (Jun 3, 2026), **high-throughput toll-free is granted through Twilio's Sales team, not the standard verification form** — engage Sales in parallel. Until verified, the number is throttled; after verification + HTTF approval, full throughput + no per-day cap unlock.

### Step 4 — Configure compliance auto-responses
Enable STOP/HELP/START auto-replies at the Messaging Service level so carrier-mandated responses fire regardless of app logic. Scope opt-out to his `creator_slug`.

### Step 5 — Wire the deploy (config, not code)
Set env on his Railway deploy (see Technical Wiring). Confirm inbound webhook lands and the brain replies as Matthew.

### Step 6 — Smoke test before launch
Buy/verify on a low-volume basis, text from a personal phone, confirm voice + compliance line + STOP works. Then announce.

---

## TFV Form — Field-by-Field

Everything below is submitted under **Fantasy Life's** identity. Items marked **[NEED]** come from them (consolidated in the next section).

| TFV Field | What goes in it |
|---|---|
| Business name | **[NEED]** Fantasy Life legal entity name (the end business) |
| Business address | **[NEED]** Fantasy Life registered address |
| Business website | `fantasylife.com` (confirm) — the web presence must corroborate Fantasy Life as the sender |
| Business contact | **[NEED]** Fantasy Life contact: name + email + phone |
| Business registration number (BRN/EIN) | **[NEED] — REQUIRED.** As of Feb 17, 2026 Twilio requires an EIN/BRN for all non-sole-proprietor toll-free verifications. Must match the legal business name. |
| Entity type | Private Profit (confirm) |
| Submitting party | **ZarBot, as the ISV** — we need an approved Primary Customer Profile for ZarBot, then register Fantasy Life as the end-business profile (recommended: dedicated subaccount). |
| Use-case category | Conversational / Customer Care (AI fantasy assistant; mixed if blasts added) |
| Use-case summary | "Fans opt in to text Matthew Berry's AI fantasy football assistant and receive AI-generated answers in his voice about rankings, start/sit, waivers, and analysis." |
| **Opt-in type** | **[NEED]** how fans consent (web form, keyword, verbal on-air, existing list) + **screenshot/description** |
| Opt-in workflow description | Step-by-step of how a fan joins and what they agree to |
| **Sample messages (2-5)** | Draft set below — confirm with Fantasy Life |
| Estimated monthly volume | **[NEED]** rough sends/month (drives throughput tier) |
| Estimated daily peak | Derived from list size + Sunday behavior |
| Privacy Policy URL | **Must represent the end business (Fantasy Life).** Use Fantasy Life's privacy/terms, or a Matthew-Berry-branded program policy. ZarBot's generic platform pages can serve as a *structural template* (our `SmsTerms.tsx` has all required clauses) but the consumer-facing policy must present as Matthew Berry/Fantasy Life's, not ZarBot's. |
| Terms / SMS Terms URL | Same as above — Matthew Berry/Fantasy Life branded. Our existing pages (`/privacy`, `/terms`, `/sms-terms`) are the content model. |
| Help / opt-out language | Provided below |

### Draft sample messages (for the TFV — confirm before submit)

1. **Welcome / opt-in confirmation:**
   `You're in! This is Matthew Berry's AI. Ask me anything fantasy — start/sit, rankings, waivers. Msg & data rates may apply. Reply STOP to opt out, HELP for help.`
2. **Typical AI reply (start/sit):**
   `Riding Puka this week — smash matchup and you don't sit your studs. Easy call.`
3. **Typical AI reply (ranking lookup):**
   `I've got Bijan as my RB2 right now, just behind CMC. Elite workload.`
4. **HELP response:**
   `This is Matthew Berry's AI fantasy assistant. Ask any fantasy football question. Reply STOP to unsubscribe. Support: [support email].`
5. **STOP response:**
   `You're unsubscribed from Matthew Berry's AI and won't receive more messages. Reply START to rejoin.`

### Opt-out / help wording (carrier-standard)

- STOP/STOPALL/UNSUBSCRIBE/CANCEL/END/QUIT → unsubscribe confirmation.
- HELP/INFO → help message with business identity + support contact.
- Compliance line on first message: `Msg & data rates may apply. Reply STOP to opt out.`

---

## Opt-In Design (the long pole for verification)

Carriers reject TFVs with weak/unclear consent. We need ONE clear, documentable opt-in. Options, in order of cleanliness:

1. **Web opt-in form** on a Fantasy Life page ("Enter your number to text Matthew's AI") with a checkbox consenting to recurring automated SMS + links to Privacy/Terms. **(Recommended — easiest to screenshot and approve.)**
2. **Keyword opt-in** — fans text a keyword to the toll-free number to start (e.g., "Text BERRY to 1-833-..."). Self-documenting.
3. **Migrating an existing list** — if Fantasy Life already has consented SMS subscribers, we need proof of how that consent was obtained.

**[NEED]** which of these (or combination) they'll use, plus the screenshot/hosted page for the submission.

---

## Technical Wiring (config, not new code)

His dedicated deploy reuses the existing engine; only env/config differs:

| Item | Value |
|---|---|
| `CREATOR_SLUG` | `matthew_berry` (confirm slug) |
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` | His (sub)account creds |
| `TWILIO_MESSAGING_SERVICE_SID` | His dedicated Messaging Service (Step 2) |
| `TWILIO_WEBHOOK_BASE` | His main-app public URL |
| Inbound webhook | `/twilio/webhook` (dedicated deploy → global brain, no `?slug=` needed since one creator per process) |
| DB | Separate PostgreSQL for Fantasy Life |
| Creator config | `creator_config/matthew_berry.json` (voice/persona — separate workstream) |

- `phone.py` / `twilio_numbers.py` attach to whatever `TWILIO_MESSAGING_SERVICE_SID` is set — no shared-brand contamination.
- The adapter already supports per-creator `from_number`; outbound sends from his toll-free.
- STOP/opt-out scoped per `creator_slug` (verify, per plan 07/09).

---

## Information We Need (from Matthew / Fantasy Life)

**Correction (important):** toll-free verification must name **Fantasy Life as the end business**, with their **EIN** (required since Feb 2026). We (ZarBot) submit + operate as the ISV, but we cannot register under our own brand. So we genuinely need the following from them. We still handle 100% of the Twilio work and operation — they just provide identity inputs and a green light.

### From Fantasy Life — REQUIRED for the verification
- [ ] Legal entity name (exact, as registered)
- [ ] **EIN / business registration number** (must match the legal name) — hard requirement
- [ ] Registered business address
- [ ] Business website (confirm `fantasylife.com`)
- [ ] Business contact: name, email, phone
- [ ] Entity type (e.g. Private Profit)
- [ ] Privacy Policy + Terms **hosted on Fantasy Life's domain** (Twilio-confirmed requirement — not just our domain), explicitly stating **mobile opt-in data is not sold or shared**. Either they add an SMS clause to their existing `fantasylife.com` privacy policy, or they host a Matthew-Berry-program page we draft (our `SmsTerms.tsx` is the content template). Either way it must live on their domain.
- [ ] Authorization for ZarBot to register + operate the number as their ISV

### Opt-in (we build; needs his input on method)
- [ ] Decision on opt-in method (text-to-join keyword vs. web sign-up form — see Opt-In Design)
- [ ] The consent point must **name "Matthew Berry"** as the sender and **disclose ZarBot** as the technology provider; we make it live + screenshot-able for the submission
- [ ] If migrating an existing SMS list: how/when consent was originally collected

### Product / voice (feeds config + sample messages, parallel workstream)
- [ ] Sign-off on the draft sample messages above (tone accurate to Matthew?)
- [ ] Approved display name / how the bot refers to itself
- [ ] Any topics, players, or takes that are **off-limits**
- [ ] What he wants the bot to push/link (rankings product, podcast, FantasyLife+, articles)

### Volume / scale
- [ ] Rough subscriber count he'd point at this
- [ ] Whether **proactive blasts** are in scope (changes use-case + throughput planning) or reactive-only
- [ ] Target go-live date (drives how early we submit — verification has lead time)

### Accounts / access
- [ ] Who owns the Twilio account — us on their behalf, or their existing Twilio? (decide subaccount structure)
- [ ] Confirmation Fantasy Life is OK being the registered/responsible business

---

## Timeline

| Phase | Duration | Notes |
|---|---|---|
| **ISV Primary Customer Profile approval** | **few business days** | **Gates everything — start first, in parallel with gathering info.** |
| Gather info from Fantasy Life | depends on them | **The other bottleneck.** |
| Build opt-in page + URLs | days | Can run while gathering info |
| Buy number + Messaging Service + submit TFV | <1 day once info is in | |
| **TFV review** | **typically 5-10 days** (Twilio-confirmed) | Varies with carrier backlog + application completeness |
| **High-throughput request (Twilio Sales)** | engage early, in parallel | HTTF is granted via Sales, not the standard form |
| Bot build + voice config + RAG ingest | parallel | The fast part on our side |
| Smoke test + launch | days | After verification clears |

**Submit as early as possible ahead of any seasonal launch** — verification is the long pole, not engineering.

---

## Costs (rough)

- Toll-free number: ~$2/month
- TFV / high-throughput verification: free / nominal
- Per-message: standard Twilio SMS/MMS rates (volume-based)
- AI/LLM cost: tracked per-message via `cost_logger` (already built)
- **No** $500-1,000/mo short-code lease

---

## Risks & Gotchas

1. **Weak opt-in = rejection.** The #1 TFV failure mode. Get the consent flow clean and screenshot-ready before submitting.
2. **Sample messages must reflect AI replies**, not just the welcome line — carriers check that what's sent matches what's registered.
3. **Pre-verification throttling.** The number works but is capped until verified; don't announce/launch before verification clears.
4. **Blast scope creep.** If proactive blasts get added later, revisit the use-case classification and throughput.
5. **Brand ownership.** Do not run his traffic under our shared platform identity — must be Fantasy Life's verified business.

---

## Open Questions

1. Twilio account ownership: their existing Twilio, or we create/manage a subaccount on their behalf?
2. Reactive-only at launch, or blasts from day one? (use-case + sample-message implications)
3. Final creator slug (`matthew_berry`?) and display name the bot uses.
4. Go-live target — is this aimed at a specific point in the NFL season?
5. Does Fantasy Life have an existing consented SMS list we'd migrate, or start fresh with new opt-ins?

---

_Next step: send the "Information We Need" checklist to Brian / Fantasy Life. Nothing gets submitted to Twilio until that comes back._
