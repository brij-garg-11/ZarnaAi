# Zarna AI

Managed AI SMS fan-engagement platform for creators. Fans text a creator's dedicated phone number and get AI-generated replies that sound and feel like the creator. The operator (Brij) runs all infrastructure as a managed service.

**Currently live:** Zarna Garg — ~4,500 subscribers.

---

## How It Works

One codebase, deployed once per client on Railway. A `CREATOR_SLUG` environment variable tells the app which creator config and training data to load. Each client gets a fully isolated database — fan conversations from one creator never touch another's.

For the full multi-client architecture, see [`CLIENTS.md`](./CLIENTS.md).

---

## Project Structure

```
app/                    ← Main Flask app (SMS pipeline, admin, analytics, live shows)
  brain/                ← AI response generation, RAG retrieval, LLM calls
  messaging/            ← Twilio + SlickText adapters
  admin/                ← Admin dashboard Flask blueprint
  analytics/            ← Analytics blueprint
  live_shows/           ← Live show, quiz, and blast features
  smb/                  ← SMB portal blueprint
  storage/postgres.py   ← All DB schema and queries
operator/               ← Separate Flask service: HQ dashboard, cron jobs, billing
  railway.*.toml        ← One Railway cron config per scheduled job
lovable-frontend/       ← React + Vite + TypeScript + shadcn/ui (calls operator API)
creator_config/         ← JSON voice configs, one per client
training_data/          ← RAG chunks + embeddings, one set per client
scripts/                ← Ingestion, backfill, and one-off tooling scripts
tests/                  ← Pytest test suite
docs/                   ← Business, engineering, ops, and security documentation
clients/                ← Per-client notes and deployment checklists
```

---

## Local Development

### Prerequisites

- Python 3.11+
- PostgreSQL (or a connection string to a dev DB)
- Node.js 18+ (for the frontend)

### Backend Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and fill in environment variables (ask Brij for dev credentials)
cp .env.example .env

# Run the main app
python main.py
# or: gunicorn main:app
```

### Operator Service

The operator is a separate Flask app:

```bash
cd operator
pip install -r requirements.txt
python main.py
```

### Frontend

```bash
cd lovable-frontend
npm install
npm run dev
```

### Tests

```bash
pytest tests/
```

Tests that require a live DB will be skipped without a `DATABASE_URL` env var set.

---

## Key Environment Variables

| Variable | Purpose |
|---|---|
| `CREATOR_SLUG` | Which creator config to load (e.g. `zarna`) |
| `DATABASE_URL` | PostgreSQL connection string |
| `TWILIO_ACCOUNT_SID` / `TWILIO_AUTH_TOKEN` | Twilio credentials |
| `SLICKTEXT_API_KEY` | SlickText credentials |
| `GOOGLE_API_KEY` | Google GenAI key |
| `OPENAI_API_KEY` | OpenAI key |
| `ANTHROPIC_API_KEY` | Anthropic key |

Never commit `.env` or any credentials to the repo.

---

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the full contributor guide — branch naming, PR process, and what not to touch.

---

## Deployment

Hosted on [Railway](https://railway.app). Two services per client:
1. **Main app** — `railway.toml` + `Dockerfile`
2. **Operator** — `operator/railway.toml` + `operator/Dockerfile`

Cron jobs are configured in `operator/railway.*.toml` (one file per job).
