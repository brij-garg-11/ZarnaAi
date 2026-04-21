# Bot Creation & Onboarding Architecture
_Blueprint — do not build until this doc is approved_

---

## Guiding principles

1. **One file, one job.** No file does more than one thing. The onboarding route only saves to DB. The phone buyer only buys phones. The ingestion module only ingests.
2. **The API route stays thin.** `api_onboarding_submit` saves data and fires a background job. That's it. No Twilio calls, no scraping, no file writing inside a route.
3. **Every step is idempotent.** If provisioning fails halfway, we can retry from the failed step without re-buying a number or re-scraping a site.
4. **Status is always visible.** At every step, `bot_configs.status` reflects exactly where the creator is. Frontend can poll it.
5. **No hardcoded creator logic.** Every new file reads from config/DB. Zarna is just another slug.

---

## Status lifecycle

```
submitted → provisioning → live
                ↓
         provisioning_failed  (with error_message stored)
```

| Status | Meaning | Frontend shows |
|---|---|---|
| `submitted` | Form saved, provisioning not started yet | "We're reviewing your submission" |
| `provisioning` | Background job running | "Setting up your bot…" spinner |
| `live` | Everything done, bot is active | Dashboard with phone number |
| `provisioning_failed` | Something broke | "Setup hit a snag — retry" button |

---

## Data that gets created per creator

```
operator_users row
  └── creator_slug, account_type, phone_number (filled after provisioning)

bot_configs row
  └── config_json (from onboarding form), status

creator_config/<slug>.json
  └── AI personality file (generated from config_json + LLM call)

creator_embeddings (DB table, pgvector)
  └── RAG chunks from their website/docs/bio

contacts table
  └── Empty at start, fans get added as they text in
```

---

## File structure

```
operator/app/
├── routes/
│   └── api.py                    ← stays thin: save form → fire job → return
│
└── provisioning/                 ← NEW MODULE (one job per file)
    ├── __init__.py               ← provision_new_creator() — orchestrates steps
    ├── phone.py                  ← buy Twilio number + configure webhook
    ├── config_writer.py          ← generate creator_config/<slug>.json via LLM
    ├── ingestion.py              ← scrape website → chunk → embed → store pgvector
    └── notifications.py          ← send welcome email via Resend

app/retrieval/
├── base.py                       ← BaseRetriever (already exists)
├── embedding.py                  ← EmbeddingRetriever file-based (keep for Zarna)
└── pg_retriever.py               ← NEW: PgRetriever — pgvector, per-slug
```

---

## Each file's exact responsibility

### `operator/app/provisioning/__init__.py`
**Only job:** Orchestrate the steps in order. Handle failures. Update status.

```python
def provision_new_creator(user_id: int, slug: str, config: dict) -> None:
    """
    Called in a background thread after onboarding submit.
    Runs all provisioning steps in order. Updates bot_configs.status at each checkpoint.
    If any step fails, sets status='provisioning_failed' and logs the error.
    Each step is idempotent — safe to retry.
    """
    set_status(slug, "provisioning")
    try:
        phone_number = phone.buy_and_configure(slug)
        store_phone_number(user_id, phone_number)

        config_writer.generate_and_write(slug, config)

        ingestion.run(slug, config)

        notifications.send_welcome(user_id, phone_number)

        set_status(slug, "live")
    except Exception as e:
        set_status(slug, "provisioning_failed", error=str(e))
        logger.exception("Provisioning failed for slug=%s", slug)
```

---

### `operator/app/provisioning/phone.py`
**Only job:** Buy a Twilio number and wire the webhook.

```python
def buy_and_configure(slug: str) -> str:
    """
    1. Search for available US local number
    2. Purchase it
    3. Set sms_url to /smb/inbound?tenant=<slug>
    4. Add to messaging service (A2P campaign)
    Returns: the purchased phone number e.164 string
    """
```

**Inputs:** `slug`
**Outputs:** `phone_number` (e.g. `+15551234567`)
**External calls:** Twilio REST API only
**Never touches:** DB, files, AI

---

### `operator/app/provisioning/config_writer.py`
**Only job:** Generate `creator_config/<slug>.json` from onboarding data using an LLM call.

```python
def generate_and_write(slug: str, config: dict) -> None:
    """
    1. Load creator_config/TEMPLATE.json
    2. Build a prompt with: display_name, bio, tone, website_url, extra_context
    3. Call Gemini to fill in: style_rules, tone_examples, guardrails, keywords
    4. Merge LLM output with template
    5. Write to creator_config/<slug>.json
    """
```

**Inputs:** `slug`, `config` dict from onboarding form
**Outputs:** writes `creator_config/<slug>.json`
**External calls:** Gemini API (1 call)
**Never touches:** Twilio, DB embeddings, email

---

### `operator/app/provisioning/ingestion.py`
**Only job:** Scrape content, chunk, embed, store in pgvector.

```python
def run(slug: str, config: dict) -> None:
    """
    1. Scrape website_url (if provided) — homepage, about, FAQ pages
    2. Process any uploaded docs (PDF/txt from onboarding)
    3. Seed high-priority "facts" chunks from bio + extra_context directly
    4. Chunk all text (500 token chunks, 50 token overlap)
    5. Embed in batches via Gemini embedding API
    6. INSERT INTO creator_embeddings (creator_slug, chunk_text, embedding, source)
    7. Mark ingestion complete
    """
```

**Inputs:** `slug`, `config` dict
**Outputs:** rows in `creator_embeddings` DB table
**External calls:** requests (scraping) + Gemini embedding API
**Never touches:** Twilio, config files, email

**Sources hierarchy (affects retrieval weight later):**
| Source tag | Content | Weight |
|---|---|---|
| `facts` | Bio + extra_context from form | 1.35 (highest) |
| `website_about` | About page | 1.20 |
| `website_faq` | FAQ page | 1.15 |
| `website_general` | Rest of site | 1.00 |
| `doc_upload` | PDFs/docs they uploaded | 1.10 |

---

### `operator/app/provisioning/notifications.py`
**Only job:** Send emails.

```python
def send_welcome(user_id: int, phone_number: str) -> None:
    """
    Send "you're live" email via Resend with:
    - Their Zar phone number
    - Link to dashboard
    - "First 3 things to do" quick start guide
    """
```

**Inputs:** `user_id`, `phone_number`
**Outputs:** email sent
**External calls:** Resend API only
**Never touches:** Twilio, DB, files

---

### `app/retrieval/pg_retriever.py`
**Only job:** Retrieve relevant chunks from pgvector for a given slug + query.

```python
class PgRetriever(BaseRetriever):
    """
    DROP-IN replacement for EmbeddingRetriever.
    Implements the same BaseRetriever interface.
    Scoped to one creator_slug — no cross-creator leakage possible.
    """
    def __init__(self, slug: str):
        self.slug = slug

    def get_relevant_chunks(self, query: str, k: int = 7) -> list[str]:
        # 1. Embed query via Gemini
        # 2. SELECT chunk_text FROM creator_embeddings
        #    WHERE creator_slug = self.slug
        #    ORDER BY embedding <=> query_vec LIMIT k
        # 3. Return list of chunk texts
```

**Inputs:** `slug`, `query`
**Outputs:** list of relevant text chunks
**External calls:** Gemini (embed query) + Postgres (vector search)

---

## How `api_onboarding_submit` changes

**Before (current):** saves to DB, returns JSON.
**After:** saves to DB, fires background thread, returns JSON. One extra line.

```python
# At the END of api_onboarding_submit, after the DB transaction commits:
import threading
from ..provisioning import provision_new_creator

threading.Thread(
    target=provision_new_creator,
    args=(user_id, creator_slug, config_json),
    daemon=True,
).start()
```

The route itself stays under 80 lines. All provisioning complexity lives in its own module.

---

## New DB table needed

```sql
-- In operator/app/db.py migrations:

-- 1. Enable pgvector (one-time, run on Railway)
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Embeddings table (multi-tenant RAG)
CREATE TABLE IF NOT EXISTS creator_embeddings (
    id           BIGSERIAL PRIMARY KEY,
    creator_slug TEXT NOT NULL,
    chunk_text   TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT 'general',
    embedding    vector(768),        -- Gemini embedding-001 dimension
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ce_slug ON creator_embeddings(creator_slug);
CREATE INDEX IF NOT EXISTS idx_ce_embedding ON creator_embeddings
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- 3. Add provisioning error tracking to bot_configs
ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS error_message TEXT;
```

---

## Retry / error handling strategy

If provisioning fails:
1. `bot_configs.status` = `'provisioning_failed'`, `error_message` = exception string
2. Frontend shows retry button
3. `POST /api/provisioning/retry` re-runs `provision_new_creator` from scratch
4. Each step checks if already done (idempotency):
   - Phone: check if `operator_users.phone_number` already set → skip
   - Config: check if `creator_config/<slug>.json` already exists → skip
   - Ingestion: check if `creator_embeddings` rows exist for slug → skip
   - Email: always resend (idempotent enough)

---

## What this looks like for a new signup (end to end)

```
1. Creator fills out onboarding form on Lovable
2. POST /api/onboarding/submit
   → DB: bot_configs.status = 'submitted'
   → Background thread starts
3. Thread: provision_new_creator()
   → DB: status = 'provisioning'
   → phone.py: buys +1 (XXX) XXX-XXXX, wires webhook (~2 sec)
   → config_writer.py: LLM generates their personality JSON (~5 sec)
   → ingestion.py: scrapes site, embeds content, stores in pgvector (~60 sec)
   → notifications.py: sends "you're live" email (~1 sec)
   → DB: status = 'live'
4. Lovable frontend polls GET /api/provisioning/status
   → Shows spinner while 'provisioning'
   → Shows dashboard when 'live'
5. Creator lands on dashboard showing their phone number
   Total time from form submit to live bot: ~70 seconds
```

---

## Migration plan for Zarna (existing creator)

1. Enable pgvector extension on Railway Postgres
2. Run one-time script: read `training_data/zarna_chunks.json` → INSERT into `creator_embeddings` with `creator_slug='zarna'`
3. Change Zarna's handler to use `PgRetriever('zarna')` instead of `EmbeddingRetriever`
4. Verify quality is identical (run test queries, compare top chunks)
5. Keep old `zarna_embeddings.json.gz` as backup for 30 days, then delete

---

## Build order (when ready)

1. `operator/app/db.py` — add pgvector migration + creator_embeddings table
2. `app/retrieval/pg_retriever.py` — new retriever
3. `operator/app/provisioning/__init__.py` — orchestrator (stub the steps first)
4. `operator/app/provisioning/phone.py` — Twilio (needs campaign SID first)
5. `operator/app/provisioning/config_writer.py` — LLM config generation
6. `operator/app/provisioning/ingestion.py` — scrape + embed + store
7. `operator/app/provisioning/notifications.py` — welcome email
8. Wire `api_onboarding_submit` to fire the background thread
9. Add `GET /api/provisioning/status` endpoint
10. Migrate Zarna to pgvector

_Total: ~500 lines across 8 files. No existing file grows significantly._
