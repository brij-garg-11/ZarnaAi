# Contributing to Zarna AI

Welcome. This is a live production system serving real fans, so changes need to be thoughtful. This guide covers everything you need to contribute safely.

---

## Before You Start

- Read [`README.md`](./README.md) to understand what the project is
- Read [`CLIENTS.md`](./CLIENTS.md) to understand the multi-tenant architecture
- Get your dev credentials from Brij (`.env` values)
- Set up your local environment (see `README.md` → Local Development)

---

## Branching

Always branch off `main`:

```bash
git checkout main && git pull
git checkout -b feat/your-thing
```

Branch naming:

| Prefix | Use for |
|---|---|
| `feat/` | New features |
| `fix/` | Bug fixes |
| `chore/` | Deps, config, refactors, cleanup |
| `docs/` | Documentation only |

Examples: `feat/fan-notes-tab`, `fix/blast-opt-out-count`, `chore/bump-openai`.

---

## Making Changes

### Backend (Python/Flask)

- Follow the patterns in `app/` — blueprints, admin submodules, helpers
- See `.cursor/rules/file-organisation.mdc` for when to create new files vs. extending existing ones
- New admin dashboard tabs → `app/admin/<tab_name>.py`, registered in `app/admin/__init__.py`
- New cron/scripts → `scripts/`, new Railway cron config → `operator/railway.<name>.toml`

### Database Changes

- All schema and queries live in `app/storage/postgres.py`
- Only **append** new column/table definitions to existing migration tuples — never restructure existing ones
- Ask Brij before touching anything that modifies existing tables

### Frontend (React/TypeScript)

- Lives in `lovable-frontend/`
- Calls the operator API
- Standard React + shadcn/ui patterns — look at existing components before introducing new ones

---

## No-Go Zones

Do not modify these without an explicit discussion with Brij:

| Area | Risk |
|---|---|
| Twilio/SlickText webhook handlers in `main.py` | Live SMS pipeline — mistakes mean fans get wrong or no replies |
| `operator/railway.*.toml` cron configs | Run against live prod DBs — a wrong cron schedule causes real data issues |
| `app/storage/postgres.py` (restructuring) | Live schema — bad migrations can corrupt fan data |
| `training_data/` | Large binary files + embeddings — changes require re-running ingestion |
| `creator_config/*.json` | Live voice configs — a bad change affects how the AI sounds to all fans instantly |
| `.env` / any credentials | Never commit secrets |

---

## Opening a Pull Request

1. Make sure your branch is up to date with `main`: `git pull origin main`
2. Run tests locally: `pytest tests/`
3. Push your branch and open a PR on GitHub against `main`
4. Fill out the PR template — every section
5. All CI checks must pass before requesting review
6. Request a review from **@brij-garg-11** (also auto-assigned via CODEOWNERS)
7. Do not merge your own PRs — wait for approval
8. If you push new commits after approval, re-request review

Keep PRs small and focused. A PR that does one thing is reviewed faster and is easier to revert if something goes wrong.

---

## Testing

- Tests live in `tests/` and use pytest
- Run: `pytest tests/`
- Tests requiring a live DB need `DATABASE_URL` set in your `.env`
- When you add a non-trivial feature, add a test for it

---

## Commit Messages

Lowercase, imperative, specific:

```
feat: add quality score column to fan profile
fix: handle empty phone in blast sender
chore: upgrade openai to 1.56.0
docs: document RAG pipeline architecture
```

---

## Getting Help

If you're unsure about something — especially anything that touches Twilio, the DB, or cron jobs — ask Brij before making the change. It's always better to ask than to accidentally disrupt a live creator's fans.
