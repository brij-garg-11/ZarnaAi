# Our Proprietary Technology — Core Pillars

*What makes Zarna AI genuinely defensible and different from generic SMS tools.*

---

## 1. Great Replies That Drive Engagement (+ Self-Improvement Loop)

We don't just auto-respond — we generate short, on-brand replies that feel personal and keep conversations going. The engine is a full RAG pipeline: intent classification → retrieval from curated content → generation via Gemini, all tuned to the creator's voice.

What makes this proprietary is the **self-improvement loop**: weekly AI quality digests surface where replies underperformed, feeding back into retraining and prompt refinement. Over time the bot gets measurably better for each creator.

**Where it lives:** `app/brain/` (intent, routing, generator, memory) + `app/admin/quality.py` (quality review + Notion integration)

---

## 2. Easy Blasting + Conversion Tracking

We make it dead simple for operators to send mass messages and see exactly what drove conversions. Blasts can be scheduled, segmented, and previewed — and every outbound link is tracked so we know what clicked, when, and who converted.

This turns SMS from a one-way megaphone into a **measurable revenue channel**.

**Where it lives:** `app/messaging/broadcast.py`, `operator/app/routes/blast.py`, `app/link_tracker.py`, Conversions tab in admin

---

## 3. Bots Know When and How to Sell

The bot doesn't pitch blindly — it reads intent and context to decide the right moment to surface a show link, podcast episode, book, merch, etc. Live show keyword flows are a prime example: a fan texts a keyword at a show, and the bot immediately routes them into the right funnel.

This is **context-aware selling**, not spray-and-pray.

**Where it lives:** `app/brain/routing.py`, `app/brain/intent.py`, `app/live_shows/` (keyword signup, broadcast worker)

---

## 4. Smart Audience Segmentation (No Spam at Scale)

We tag and segment fans by behavior, location, event attendance, and engagement level. Blasts go to the right slice of the audience — not everyone, every time. This keeps opt-out rates low and engagement rates high, which is the whole game at scale.

**Where it lives:** `app/storage/postgres.py` (fan tags + segment tables), Audience tab in admin (filters, CSV export), blast targeting logic in operator

---

## The Moat in One Sentence

We combine a **creator-voice AI reply engine** with **audience segmentation**, **conversion tracking**, and **context-aware selling** — and we improve all four over time through a closed feedback loop. No generic SMS platform does all of this, and certainly not tuned to a specific creator's brand.
