# Bot Behavior Fixes — Implementation Plan

**Status:** Approved, not yet implemented
**Author:** Eng (via review of last 1,000 fan↔bot messages, Jul 7–9 2026)
**Owner/Reviewer:** @brij-garg-11

Origin: a review of recent Zarna AI conversations surfaced troubling behavior.
This plan covers the 4 items chosen for fixing. Fact-accuracy brush-up and
content-safety edge cases were reviewed and deliberately deferred/accepted.

---

## Locked decisions

| # | Fix | Decision |
|---|-----|----------|
| 1 | Tour-date contradictions | `creator_config/zarna.json` `upcoming_shows` is the single source of truth. Stop using Bandsintown. Only mention shows/tickets when the fan asks. |
| 2 | AI identity | Confirm it's an AI whenever asked (who is this / is this AI / are you real). Never claim to be the real Zarna. No disclaimer on every message. |
| 3 | Fact accuracy | Deferred — brush up later. Not in this plan. |
| 4 | Double-texting | Fix cross-worker duplicate webhooks only (Postgres dedup). SMS 2-bubble link split and rapid-fire batching are OUT of scope. |
| 5 | Content-safety edge cases | Accepted as-is. Not in this plan. |
| 6 | Emotional/crisis handling | Hard gate for explicit self-harm/suicide only → deterministic 988 response, bypass LLM. Keep empathetic comedy for general sadness. Log crisis events and surface them for human follow-up on the **zar.bot** site. |

---

## No-go-zone cautions (read before editing)

Per `.cursor/rules/dev-workflow.mdc` these touch live production; each gets its
own reviewed PR, tests, and careful logging:

- `main.py` Twilio/SlickText webhook handlers (dedup wiring)
- `app/storage/postgres.py` (append migrations to existing tuples ONLY — no new logic)
- `creator_config/zarna.json` (live voice config — changes affect all fans instantly)

`app/admin/__init__.py` and `app/storage/postgres.py` are on the "never grow"
list — delegate to submodules / append-only migrations.

---

## Confirmed architecture facts (verified during planning)

- `Procfile`: `gunicorn main:app --workers 8 --preload`. Dedup is currently a
  per-worker in-memory LRU (`main.py` ~292–312) → the same webhook hitting two
  of the 8 workers produces two independent LLM generations = the "clashing
  double replies." **Root cause of double-texting.**
- No Redis anywhere in code. Postgres is the only shared store.
- Tour data: `app/brain/show_calendar.py` fetches Bandsintown first (wins when it
  returns data), else `creator_config/zarna.json` `upcoming_shows` (31 shows,
  incl. TWO "Portland" entries: OR Jul 2–4, ME Dec 4). SHOW intent uses the
  calendar directive; GENERAL/QUESTION intents freestyle dates via RAG/LLM →
  source of both non-sequitur plugs and self-contradiction ("no shows Dec 4").
- Identity: prompt says "AI assistant" but also "write in Zarna's voice"; no hard
  rule against first-person-as-real-person; "who is this" not in the AI fast-path.
- No crisis gate and no hardcoded 988 exist. The good 988 reply seen in logs was
  emergent LLM behavior, not guaranteed. Tone defaults to `roast_playful` when
  keyword heuristics miss.
- Operator (`zar.bot` backend) `get_conn()` connects to the same client Postgres
  DB that holds `messages`/`contacts` (see `operator/app/routes/api.py`
  `inbox_thread` ~787). So a `safety_flags` table written by the main app is
  directly readable by the operator API — no cross-DB sync needed.

---

## Fix 1 — Tour-date contradictions

**Goal:** No self-contradiction, no non-sequitur tour plugs, no invented dates or
fan locations.

### Changes
1. **Config as source of truth** — `app/brain/show_calendar.py`
   - Stop using the Bandsintown API as primary/override. Build the calendar only
     from `creator_config/zarna.json` `upcoming_shows` + the existing date-aware
     past/upcoming split.
   - Keep the fetch code path removable/behind a disabled flag rather than
     deleting outright, in case it's wanted later. Default: off.
2. **Two-Portland disambiguation** — `show_calendar.py` `_match_city` (~400)
   - Match on city **+ state** when a state is present/inferable.
   - When only a city is given and several match, return the **soonest upcoming**.
   - Never return an already-ended show as "upcoming."
3. **No dates outside SHOW intent** — `app/brain/generator.py` + `creator_config/zarna.json`
   - Remove/neutralize the generic tickets fact from RAG context for non-SHOW intents.
   - Add hard guardrail text: *"Never volunteer tour dates, cities, or the tickets
     link unless the fan is specifically asking about live shows/tour. Never state
     or deny a specific show date unless it comes from the provided Show guidance."*
   - In the SHOW prompt, add: *"Do not deny or invent shows; only use the Show
     guidance provided."*
4. **No location hallucination** — `app/brain/generator.py`
   - Gate Variant B's "reference their city" instruction to only fire when
     `fan_location` is actually stored; instruct model never to guess a location
     from context / area code.

### Files
- `app/brain/show_calendar.py`
- `app/brain/generator.py`
- `creator_config/zarna.json`

### Tests (`tests/test_show_calendar.py`, extend)
- Config authoritative (Bandsintown ignored/off).
- "Portland" + state disambiguation; soonest-upcoming tie-break.
- Unknown city → clearly "not booked yet."
- Non-SHOW intent never emits a specific date or tickets link.

---

## Fix 2 — AI identity honesty

**Goal:** Always confirm it's an AI when asked; never claim to be the real person;
no per-message disclaimer.

### Changes
1. **Fast-path identity questions** — `app/brain/intent.py`
   - Add "who is this", "who are you", "who's this" to `_AI_QUESTION_PHRASES`
     (route deterministically to QUESTION).
2. **Hard identity guardrail** — `creator_config/zarna.json` `hard_fact_guardrails`
   + fallback `_HARD_FACT_GUARDRAILS` in `app/brain/generator.py`:
   - *"You are an AI version of Zarna, not the real person. If asked who/what you
     are, whether you're AI, a bot, or real — always clearly confirm you're
     Zarna's AI. Never claim to literally be the real Zarna or that a human is
     texting."*
3. **Consistent identity answer** — replace first-person openers like "It's Zarna
   Garg texting you!" with a consistent line, e.g. *"This is Zarna Garg's AI —
   trained on her voice and comedy."* Normal chat still uses her voice.

### Files
- `app/brain/intent.py`
- `app/brain/generator.py`
- `creator_config/zarna.json`

### Tests (new `tests/test_ai_identity.py`)
- "who is this" / "is this AI" / "are you real" → AI confirmation.
- Never asserts being the real human.

---

## Fix 4 — Double-texting (clashing duplicate replies)

**Goal:** One inbound webhook → exactly one generation, across all 8 workers.
Scope limited to cross-worker dedup (per decision).

### Changes
1. **Persistent Postgres dedup** — `app/storage/postgres.py`
   - Append a migration (existing migrations tuple) for:
     `processed_messages (message_id TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW())`.
   - Add storage helper `mark_processed(message_id) -> bool` using
     `INSERT ... ON CONFLICT DO NOTHING` + rowcount check (atomic across workers).
     Returns True if this caller claimed it (first), False if duplicate.
2. **Wire into `main.py` `_already_processed`**
   - Keep in-memory LRU as cheap first check, then fall back to the atomic DB claim.
   - Empty/missing `message_id`: synthesize a stable key
     (`channel:sender:sha1(body)[:16]`), matching the existing SlickText v2 pattern.
3. **Pruning** — opportunistic delete of `processed_messages` rows older than ~7
   days (inline occasionally, or note a tiny cron). Keep an index on `created_at`.

### Files
- `app/storage/postgres.py` (migration + helper)
- `main.py` (dedup wiring — SMS pipeline no-go zone: additive only)

### Tests (new `tests/test_dedup.py`)
- Same `message_id` claimed once across simulated concurrent callers.
- Empty-id synthesis is stable and dedupes identical resends.
- Skips gracefully when no `DATABASE_URL` (matches existing test conventions).

### Explicitly NOT doing (per decision)
- Keeping ticket link inline to avoid the 2-bubble SlickText split.
- Debouncing/batching rapid consecutive fan messages.

---

## Fix 6 — Crisis handling + flagging + zar.bot review tab

**Goal:** Genuine self-harm/suicide statements get a reliable, warm, no-jokes
response with crisis resources, bypassing the comedy LLM; each event is logged
and reviewable by a human on the zar.bot dashboard. General sadness is untouched
(keeps empathetic comedy) to avoid being heavy-handed.

### Part A — Detection + response (main app)
1. **New module `app/brain/crisis.py`**
   - `is_crisis(message) -> bool`: high-precision matcher, explicit self-harm /
     suicidal ideation ONLY (e.g. "kill myself", "want to die", "end my life",
     "no longer alive", "suicidal", "don't want to be here"). Deliberately narrow
     to minimize false positives.
   - `get_crisis_response(creator_config) -> str`: fixed, warm, serious reply
     including **988** (US/Canada) and **Crisis Text Line 741741**. No jokes, no LLM.
2. **Early gate in `app/brain/handler.py`** (after saving the user message ~101,
   before intent/RAG/generation ~125):
   - If `is_crisis`: save the deterministic reply, insert a safety flag, set
     `tone_mode='sensitive_care'`, log `[ZARNA]` crisis line, and return early —
     no comedy generation.

### Part B — Flag storage (main app DB)
3. **Append migration** — `app/storage/postgres.py`:
   `safety_flags (id BIGSERIAL PK, phone_number TEXT, creator_slug TEXT,
   message_text TEXT, matched_at TIMESTAMPTZ DEFAULT NOW(), reviewed BOOL DEFAULT FALSE,
   reviewed_at TIMESTAMPTZ)`; index on `(creator_slug, reviewed, matched_at)`.
   - Storage helpers: `insert_safety_flag(...)`, `list_safety_flags(slug, reviewed=False)`,
     `mark_safety_flag_reviewed(id)`.

### Part C — zar.bot review tab (operator + lovable-frontend)
4. **Operator API** — `operator/app/routes/api.py` (5,200+ lines, treat carefully;
   prefer a small new route group / blueprint if practical):
   - `GET /api/safety/flags` — list unreviewed flags for the logged-in creator_slug
     (reuse `get_conn()`, `_slug_or_abort()`, phone masked to last-4, link to thread).
   - `POST /api/safety/flags/<id>/review` — mark reviewed.
   - Reads the same client DB the inbox already uses (verified).
5. **lovable-frontend** — new "Safety" page/tab (`lovable-frontend/`):
   - Calls the two endpoints; lists flagged fans, message, timestamp, deep-link to
     the existing inbox thread, and a "Mark reviewed" action. Styled like existing
     tabs (React + Vite + Tailwind + shadcn/ui).

### Files
- new `app/brain/crisis.py`
- `app/brain/handler.py`, `app/brain/tone.py` (share crisis keywords)
- `app/storage/postgres.py` (migration + helpers)
- `operator/app/routes/api.py` (+ new route group)
- `lovable-frontend/` (new Safety tab/page + API client calls)

### Tests
- new `tests/test_crisis.py`: crisis phrases hit the gate, 988/741741 present,
  LLM bypassed; general sadness ("I'm a bit lonely", "feeling sad") does NOT hit
  the hard gate and stays empathetic-comedic.
- Operator endpoint test if an operator test harness exists; otherwise manual.

---

## PR breakdown & suggested order

Branch off `main`, one logical change per PR, PR template filled, CI green,
reviewed by @brij-garg-11 (no self-merge).

1. `fix/ai-identity-disclosure` (Fix 2) — smallest, safest.
2. `fix/tour-date-source-of-truth` (Fix 1).
3. `fix/dedup-double-texting` (Fix 4) — SMS pipeline no-go zone.
4. `feat/crisis-safety-gate-and-tab` (Fix 6) — largest; spans main app + operator
   + frontend. May split into 6a (gate + flag storage in main app) and 6b (zar.bot
   API + frontend tab) if the diff is too large to review at once.

Run `pytest tests/` before each PR.

---

## Assumptions to verify during implementation

- [ ] Operator connects to the client's main DB via its `DATABASE_URL` (inbox
      confirms this). Confirm the operator deployment for the Zarna client points
      at the same DB the main app writes to, so `safety_flags` is visible.
- [ ] Confirm whether Bandsintown currently returns data (if it's already empty,
      turning it off is a no-op safety improvement; if live, config takeover is the
      actual fix).
- [ ] lovable-frontend build/deploy flow (Lovable) — confirm how to add a page and
      whether it's committed in this repo or managed via Lovable.
- [ ] Crisis response copy — get final wording approved (tone + exact resources)
      before shipping, since it's user-facing and sensitive.
- [ ] Confirm `main.py` `_already_processed` is the single choke point for BOTH
      Twilio and SlickText inbound before wiring DB dedup.
