# Client #1 — Zarna Garg

**Status:** Live  
**Launched:** 2026  
**Platform:** SMS via SlickText + Twilio  

---

## Who She Is

Zarna Garg is a stand-up comedian and author. Her AI texts back fans in her voice — sharp, high-energy, family- and culture-aware comedy. The AI is trained on her specials, podcast, skits, YouTube content, and her book *This American Woman*.

---

## What We Run for Her

| Layer | What It Is | Where It Lives |
|---|---|---|
| AI brain | Zarna's voice, tone rules, training data | `app/brain/generator.py`, `creator_config/zarna.json` |
| Training data | 2,500+ knowledge chunks from her material | `training_data/zarna_chunks.json` |
| Fan database | All phone numbers, conversations, profile memory | Railway Postgres (Zarna's deployment) |
| SMS pipeline | SlickText (primary) + Twilio (secondary) | Managed under our accounts |
| Operator dashboard | Zarna's team logs in to manage shows, blasts, audience | Railway (operator service) |
| Hosting | Two Railway services: main app + operator dashboard | Railway |

---

## Deployment

- **Main app (AI + webhooks):** Railway service `zarna-main`
- **Operator dashboard:** Railway service `zarna-operator`
- **Database:** Railway Postgres plugin, connected to both services
- **Env var:** `CREATOR_SLUG=zarna`

---

## Content We've Ingested

| Source | Status | Notes |
|---|---|---|
| YouTube channel | ✓ Ingested | Channel ID: `UC5Gb9pWYSfcpEdTb6vf9Dbg` |
| Podcast — Zarna Garg Family Podcast | ✓ Ingested | RSS feed, auto-tracks new episodes via `podcast_guids.json` |
| Stand-up specials | ✓ Ingested | Transcripts in `Transcripts/specials/` |
| Skits | ✓ Ingested | Transcripts in `Transcripts/skits/` |
| Book — *This American Woman* | ✓ Ingested | `this american woman.pdf` |
| YouTube processed transcripts | ✓ Ingested | `Processed/youtube/` |
| Raw content archive | On file | `Raw/` — Instagram, Podcast, Press, Standup, TikTok, Youtube |

---

## Voice Config

See `creator_config/zarna.json` for the full voice guide. Key notes:

- **Core angles:** Indian-mom, immigrant family, parenting, marriage
- **Shalabh / MIL / Baba Ramdev:** always stay in comedy lane even if fan seems to push
- **Family facts (hard guardrails):** husband Shalabh, kids Zoya, Brij, Veer — never invent other family
- **Banned:** honey, darling, sweetie, profanity, homophobic anything

---

## SMS Keywords

| Keyword | Platform | List | Purpose |
|---|---|---|---|
| `ZARNA` | SlickText | `zarna` | Main subscriber list (~4,146 subscribers) |
| `HELLO` | SlickText | `hello` | Secondary list (~747 subscribers) |

---

## Live Stats (as of March 2026)

- **Total subscribers:** 4,504
- **Active (7-day):** 760 (16.5% engagement rate)
- **Fans with saved memory:** 482
- **All-time messages:** 9,611

---

## Refreshing Her Knowledge Base

When Zarna drops new content (new special, new podcast episodes, new YouTube videos):

```bash
# Add new podcast episodes
python scripts/ingest_podcast.py

# Rebuild embeddings after any ingestion
python scripts/build_embeddings.py
```

For YouTube or new specials, run the corresponding ingest script first, then rebuild embeddings.

---

## Contacts

- **Creator contact:** Zarna / her team
- **Managed by:** Brij (operator)
