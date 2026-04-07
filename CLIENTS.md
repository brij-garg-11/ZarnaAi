# Multi-Client Structure

This repo powers a managed AI texting platform for creators. The same codebase is deployed once per client — each client gets their own Railway project, their own isolated database, and their own phone number. You (the operator) own and run all of it.

---

## Active Clients

| # | Client | Status | Subscribers | Deployment |
|---|---|---|---|---|
| 1 | Zarna Garg | ✅ Live | ~4,500 | `clients/zarna/` |

---

## Adding a New Client

1. Copy `clients/template/README.md` → `clients/[slug]/README.md` and fill it out
2. Copy `creator_config/TEMPLATE.json` → `creator_config/[slug].json` and fill it out
3. Follow the onboarding checklist in their README
4. Deploy a new Railway project from this repo with `CREATOR_SLUG=[slug]`

That's it. No code changes. No rebuilding anything.

---

## How It Works (One Paragraph)

Each client deployment is the same code running with different environment variables. The `CREATOR_SLUG` env var tells the app which creator config to load (`creator_config/[slug].json`) and which training data files to use (`training_data/[slug]_chunks.json`, `training_data/[slug]_embeddings.json.gz`). The database is completely separate per client — fan phone numbers, conversations, and memory from one creator never touch another's. The operator dashboard is the same app but connects to each client's own database.

---

## Folder Structure

```
creator_config/
├── zarna.json          ← Client #1 voice config
├── TEMPLATE.json       ← Copy this for each new client
└── [slug].json         ← Add one per new client

clients/
├── zarna/
│   └── README.md       ← Client #1 brief, deployment notes, content inventory
└── template/
    └── README.md       ← Copy this for each new client

training_data/
├── zarna_chunks.json         ← Client #1 knowledge base
├── zarna_embeddings.json.gz  ← Client #1 embeddings
└── [slug]_chunks.json        ← Add one pair per new client
    [slug]_embeddings.json.gz

Raw/                    ← Zarna's raw content archive (scripts read from here)
Transcripts/            ← Zarna's processed transcripts (scripts read from here)
Processed/              ← Zarna's processed output (scripts read from here)
```

> **Note:** `Raw/`, `Transcripts/`, and `Processed/` currently contain Zarna's content only. When onboarding a new client, their raw content and transcripts will live in equivalent folders or be referenced directly by path in their creator config.

---

## What the Operator Controls vs. What the Client Controls

### You Own and Run
- The codebase (all updates ship from here)
- Railway hosting for every deployment
- Every client's database (fan phone numbers, conversations, memory)
- The AI layer — voice config, training data, model selection
- The SMS pipeline (SlickText / Twilio accounts and credentials)
- All security, uptime, and infrastructure

### The Client Can Do (via their dashboard)
- Create and manage live shows / streams
- Write and send blasts to their audience (they must confirm before sending)
- View fan conversations and profiles
- Download their subscriber list as CSV

### The Client Is Responsible For
- Their own privacy policy (must be live before launch)
- Their SMS terms of service
- Their original fan opt-in consent records
- Approving all blast content before it goes out

---

## Risk Notes

See `docs/managed_service_risk.md` for the full breakdown of what risks we carry and how they're priced.
