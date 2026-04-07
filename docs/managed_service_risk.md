# Managed Service Risk & Pricing Rationale

*Internal document — for pricing decisions and client onboarding discussions*

---

## What We Own and Operate

When we take on a client, we own and operate everything:

- **The codebase** — built, maintained, and updated by us
- **The infrastructure** — hosted on our Railway account
- **The database** — we control it, back it up, and manage access
- **The SMS pipeline** — Twilio and/or SlickText accounts managed under our umbrella
- **The AI layer** — Gemini, OpenAI, and Anthropic API accounts and billing
- **The admin dashboard** — client gets access, but we run the system it sits on

The client gets a product. We carry everything underneath it.

---

## Risk We Absorb (and Price For)

### Data Liability
We store fan phone numbers, full message histories, and personal profile details. If there is a breach, we are the technical operator — we will be named first. At scale (thousands of subscribers per client), this is real PII exposure under CCPA, GDPR, and similar laws. Fines run up to $7,500 per violation. We mitigate this through infrastructure security, but we cannot eliminate the risk — we price for carrying it.

### SMS / TCPA Exposure
Automated texts carry per-message statutory damages of $500–$1,500 under the TCPA. We handle STOP filtering, rate limiting, and delivery technically — but because we control the sending infrastructure, we are operationally liable even when consent is the client's responsibility. Any bulk blast error, missed opt-out, or compliance gap touches us. We price for this exposure.

### AI Brand Risk
The AI speaks in the client's voice and goes out under their name. If it produces an offensive, wrong, or damaging reply, the client faces the public fallout — but we built and deployed the system. Negligent AI design creates shared liability. We price for the ongoing responsibility of maintaining a system that represents someone's public persona at scale.

### Infrastructure Uptime
We are on the hook when the platform goes down — especially during live shows where downtime is immediately visible to thousands of fans. We price for maintaining and monitoring a production system, not just building it once.

### Vendor Cost Volatility
AI API costs (Gemini, OpenAI, Anthropic) and SMS costs (Twilio/SlickText) are usage-based and variable. We absorb cost spikes from viral moments, fan surges, or client-side events we didn't anticipate. We price for this financial buffer.

### Credential and Access Security
We hold API keys, database credentials, and admin access for every client. A security failure on our side affects all clients simultaneously. We price for the overhead of managing secrets, rotating credentials, and maintaining access hygiene across a portfolio.

---

## What the Client Is Responsible For

Even in a fully managed model, the client retains some obligations:

- Publishing their own **privacy policy** and **SMS terms of service**
- Ensuring their **original subscriber consent** (opt-in) is legally valid
- **Approving all bulk blast content** before it sends (we require sign-off)
- Disclosing to fans that they are chatting with an **AI** (we recommend, they own)
- Any legal counsel they need around TCPA, CCPA, or data rights for their fanbase

We do not absorb liability that originates from the client's pre-existing compliance gaps.

---

## Risk Pricing Summary

| Risk Category | Who Primarily Carries It | Included in Our Fee |
|---|---|---|
| Data breach / infrastructure security | Us | Yes |
| TCPA operational compliance | Shared | Yes |
| AI reply quality and brand safety | Shared | Yes |
| Platform uptime and reliability | Us | Yes |
| AI/SMS cost volatility | Us (within reason) | Yes, with fair-use limits |
| Client's consent records and privacy policy | Client | No |
| Client-approved blast content errors | Client | No |

---

*This document is for internal pricing and business development use. Not for direct distribution to clients.*
