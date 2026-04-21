# Zarna AI — What the platform can do

A plain-language overview for managers, partners, and anyone who won’t read code. It describes what’s live in the product today.

---

## What fans experience

- **Text Zarna’s number** through your SMS provider (for example SlickText or Twilio). They get **AI replies in Zarna’s comedic voice**, informed by curated training material so answers stay on-brand.
- **Replies can feel smarter over time:** the system can remember **simple, voluntary things** someone has shared (like city or that they’re a parent), and use **tags** so you can group fans later in the admin dashboard. There are **rules to avoid storing sensitive topics** and to **protect minors**.
- **Some messages don’t get a robot reply on purpose** — for example a quick “thanks” — so the chat doesn’t feel annoyingly chatty.
- **At in-person comedy shows *or* online live streams**, fans can **text a keyword** to join a list. When you create the event, you pick a **type**:
  - **Comedy show** — after they join (keyword-only text), they get a **fun, rotating welcome SMS** in Zarna’s voice: you’re in → joke → enjoy the night (automated; same “no AI reply” on that keyword text).
  - **Live stream** — same idea, but the messages are written for **people watching from home** (welcome to the live, stream jokes, grab snacks / enjoy the stream). Still rotating and automated.
  - **Other** — they join the list **silently** (no auto welcome text) when they only text the keyword; the AI still doesn’t jump in on that keyword-only message.
- **WhatsApp** can be used where Twilio is set up for it; behavior mirrors SMS where configured.

---

## What your team can do (password-protected web dashboard)

You sign in with a **single admin password** (set on the server). Inside you’ll find:

### Home / overview

- **Numbers at a glance** — how the audience and system are doing.
- **Optional filters** — e.g. by tag or rough location text fans mentioned.
- **Health-style readouts** — things like how many webhook or AI issues have been seen recently, so you know if something’s wrong.

### Audience

- **See fan profiles** — short summaries and tags your team can use for planning or exports.
- **Filter and download** a spreadsheet (CSV) of fans when you need it.

### Conversations

- **Browse texts like an inbox** — see who messaged, peek at previews.
- **Open a full thread** with one phone number, scroll through the back-and-forth, **search** message text, and **export** that conversation to CSV if you need a record.

### Live shows & live streams

Built for **“text this word to get on the list”** — whether the audience is **in the room** or **tuned in online**:

- **Create a show** (draft), then **go live** when you’re ready. Only **one show is “live” at a time** so texts match the right keyword. When it’s over, **end** the show — the **list of numbers is saved**.
- **Pick the event type when you set it up:** **Comedy show** (in-person welcome texts), **Live stream** (stream-themed welcome texts), or **Other** (join list, no auto welcome). The dashboard labels each show so you can tell them apart.
- **How people join**
  - **Keyword:** They text your word (small typos are forgiven on longer keywords). If they **only** send the keyword, they’re added to the list and **the AI stays quiet** for that message.
  - **Time window:** During your chosen start/end window, **any** text from them can count as joining (you choose this mode when creating the show).
- **Times you care about** are entered in a **real-world timezone** (e.g. Eastern for a New York show). The system stores the exact moments correctly behind the scenes.
- **Message everyone on that show’s list** (broadcast) — with **how many people** will get it, **masked sample numbers** so you double-check, **two checkboxes** you must tick before sending, and a **send a test to my phone** step first.
- **Download** the signup list as a spreadsheet anytime.
- **Recent actions log** — e.g. when someone went live, queued a blast, or updated the window.

---

## Extras for integrations (simple explanation)

- **“Is the service up?”** — There’s a simple **health check** hosting providers can ping.
- **Programmatic testing** — With a **secret API key** (in production), other tools can send a fake inbound message and get the AI reply back as data. Useful for apps or internal tools, not something most fans see.

---

## What you still need outside this product

- **Phone numbers and texting** are billed through **SlickText and/or Twilio**; this app connects to them, it doesn’t replace their accounts or contracts.
- **Legal and compliance** for SMS (consent, opt-out, privacy notices) are **your responsibility** with your counsel — the product helps operationally but doesn’t write your policies.
- **Logins for staff** are **one shared admin password today**, not separate named accounts or “Sign in with Google.”
- **No built-in ticketing, email blasts, or full CRM** — it’s focused on **SMS/WhatsApp fan chat**, **remembering light audience context**, and **live-event / live-stream lists + blasts**.

---

## One-sentence summary

**Zarna AI lets fans text an AI that sounds like Zarna, helps you understand and export your audience, and runs text-to-join lists and safe bulk texts for in-person comedy shows and online live streams alike — including different auto-welcome texts for each.**

---

*Internal reference document. Update when you ship major new capabilities.*
