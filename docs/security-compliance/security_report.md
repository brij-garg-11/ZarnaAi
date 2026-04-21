# Zarna AI — Security & privacy (plain-language)

This note explains **how we try to keep the system and fan data safe**, and **what is still your responsibility** (hosting, passwords, legal wording). It matches how the software is built today.

---

## The big picture

- **Texts don’t go “straight to the AI.”** They arrive through **SlickText or Twilio**, which call our server with each message. If someone could **fake those calls**, they could pretend to be a fan — so we rely on **locks on those doors** (see below).
- **Your admin pages** (audience, chats, live shows) are protected by **one shared password**, not separate logins per person.
- **Keys and secrets** (AI key, database link, webhook passwords) live in the **host’s settings** (e.g. Railway). Anyone who can open that dashboard can see them — **limit who has access.**

---

## Who can open what?

**Staff dashboard & live-show tools**  
Protected by an **admin password** you choose. If you never set one, those pages stay **off**. The password itself is **not** stored in the fan database.

**Twilio texts**  
By default, Twilio proves each request is really from them (like a sealed envelope). **Leave that on** in production and use a proper **HTTPS** web address so that check stays reliable.

**SlickText texts**  
You can set an extra **shared secret**: only requests that include the right secret are accepted. **Strongly recommended in production.** If you skip it, anyone who guesses or finds your webhook URL could trigger the bot — the app will warn you in production logs.

**Optional “test API”**  
In production, a separate **secret key** is required before other systems can send fake messages and get AI replies back. That keeps random internet traffic from using your AI bill.

---

## Protecting the service from overload and mistakes

- **Duplicate texts:** If a provider sends the same message twice, we try to **ignore the duplicate** so fans don’t get double replies.
- **Rate limits:** One phone number can only drive **so many AI replies per minute** so a bug or abuse doesn’t rack up huge usage.
- **Busy periods:** There’s a cap on **how many AI conversations run at once** per server; extra traffic may get a polite “busy” instead of a reply.
- **Mass texts (live shows):** The product asks you to **confirm counts**, see **sample numbers**, and **test on your own phone** first — but **sending the wrong message to the wrong list is still a human mistake**; the tool can’t stop every error.

These help with **cost and stability**; they **don’t replace** locking down webhooks and admin access.

---

## What gets written to logs?

**Normally:** we log **summaries** — for example **last few digits of a phone** and **how long the message was**, not the full text in every line.

**Debug mode:** you can turn on **full logging** of message bodies for troubleshooting. **Turn it off again** when you’re done; don’t leave it on in production.

---

## Messages the robot ignores on purpose

Some inbound texts are **filtered out** before the AI answers — for example **STOP / START / HELP**-style words carriers care about, **reactions**, or **emoji-only** pings. That cuts noise and avoids fighting the platform.

**Live-show keywords** still work on their own path, so **“text CHICAGO to join”** isn’t accidentally swallowed by those filters.

---

## What we store about fans

When a database is connected (typical in production), we may keep:

- **Phone number** and **message history** (who said what and when).
- A **short profile summary**, **tags**, and rough **location text** (meant as city/region — **not** full street addresses in policy).
- **Live-show signups** (which show, which number, when).
- A **log of admin actions** on live shows (went live, sent blast, updated schedule, etc.) — **what happened**, not a full video of who clicked.

**Google’s AI (Gemini)** sees message text (and supporting context) to **write replies** and to **update that light profile**. Their rules and your Google account settings apply.

**SlickText and Twilio** handle the actual SMS/WhatsApp pipes under **their** privacy and telecom policies.

---

## Rules we try to follow for fan profiles

The system is instructed **not** to build rich dossiers on sensitive topics, and to **back off if someone seems under 18**. In practice this is **software + AI assistance** — it helps, but it is **not a legal guarantee** and **not a substitute** for your lawyer’s privacy policy and SMS consent language.

---

## Honest limitations (worth knowing)

- **One admin password** shared by the team — if someone leaves, **change it.**
- **SlickText** without the extra secret = **riskier** if the webhook link leaks.
- If you run **several copies** of the app at once, some counters (rate limits, duplicate detection) are **per copy**, not magically synced across the world.
- **SMS is not secret from carriers** the way a banking app might be; treat content accordingly.
- **No built-in “who clicked Send”** on every button — audit logs record **types of actions**, not named staff users.

---

## Practical checklist for whoever runs the servers

1. Use a **strong admin password** and don’t share it in email or Slack long-term.  
2. Set the **SlickText webhook secret** and match it in SlickText’s settings (if they support it).  
3. Keep **Twilio’s authenticity check** on and use **HTTPS** for your public URL.  
4. Leave **full message logging** off unless you’re actively debugging.  
5. Limit access to **Railway (or your host)** and **Google / Twilio / SlickText** dashboards; **rotate keys** after any scare.  
6. Glance at **usage dashboards** occasionally for weird spikes.

*(Your technical teammate may know these by names like `ADMIN_PASSWORD`, `SLICKTEXT_WEBHOOK_SECRET`, `API_SECRET_KEY`, and `TWILIO_VALIDATE_SIGNATURE` — same ideas.)*

---

*This document is for internal understanding. For contracts, GDPR/CCPA, or DPAs, work with qualified legal counsel.*
