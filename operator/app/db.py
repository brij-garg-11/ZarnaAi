"""
Shared Postgres connection helpers.
All queries here are safe — no raw phone numbers returned to routes.
"""

import logging
import os
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def get_conn():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    dsn = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(dsn)


def init_db():
    """
    Run idempotent migrations for operator-specific tables.

    IMPORTANT: each statement runs with autocommit=True so a failure on one
    (e.g. dropping a constraint that doesn't exist on a fresh DB) does NOT
    roll back the others.  Previously all statements ran inside a single
    transaction, which meant the stale-URL cleanup at the end never executed
    whenever an earlier ALTER TABLE failed.
    """
    statements = [
        # ── Core tables ────────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS operator_users (
            id            BIGSERIAL PRIMARY KEY,
            email         TEXT UNIQUE NOT NULL,
            name          TEXT NOT NULL DEFAULT '',
            password_hash TEXT NOT NULL,
            is_active     BOOLEAN DEFAULT TRUE,
            is_owner      BOOLEAN DEFAULT FALSE,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            last_login_at TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS broadcast_optouts (
            phone_number TEXT PRIMARY KEY,
            opted_out_at TIMESTAMPTZ DEFAULT NOW(),
            source       TEXT DEFAULT 'stop_reply'
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS blast_drafts (
            id                  BIGSERIAL PRIMARY KEY,
            name                TEXT NOT NULL DEFAULT 'Untitled draft',
            body                TEXT NOT NULL DEFAULT '',
            channel             TEXT NOT NULL DEFAULT 'twilio',
            audience_type       TEXT NOT NULL DEFAULT 'all',
            audience_filter     TEXT DEFAULT '',
            audience_sample_pct INT DEFAULT 100,
            status              TEXT NOT NULL DEFAULT 'draft',
            scheduled_at        TIMESTAMPTZ,
            sent_at             TIMESTAMPTZ,
            sent_count          INT DEFAULT 0,
            failed_count        INT DEFAULT 0,
            total_recipients    INT DEFAULT 0,
            created_by          TEXT DEFAULT '',
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS operator_blast_images (
            id         BIGSERIAL PRIMARY KEY,
            filename   TEXT NOT NULL,
            mime_type  TEXT NOT NULL DEFAULT 'image/jpeg',
            data       BYTEA,
            data_b64   TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,

        # ── Idempotent column additions ────────────────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS media_url TEXT DEFAULT ''",
        # link_url: raw URL entered by operator; tracked_link_slug: the /t/<slug> we created
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS link_url TEXT DEFAULT ''",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS tracked_link_slug TEXT DEFAULT ''",
        # tracked_link_clicks: shared with main app — ensure it exists here too
        # so the operator's /t/<slug> redirect can log clicks independently
        """
        CREATE TABLE IF NOT EXISTS tracked_link_clicks (
            id         BIGSERIAL PRIMARY KEY,
            link_id    BIGINT NOT NULL REFERENCES tracked_links(id) ON DELETE CASCADE,
            clicked_at TIMESTAMPTZ DEFAULT NOW(),
            ip_hash    TEXT DEFAULT '',
            ua_short   TEXT DEFAULT ''
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tlc_link_id ON tracked_link_clicks(link_id)",
        "CREATE INDEX IF NOT EXISTS idx_tlc_clicked_at ON tracked_link_clicks(clicked_at)",
        # phone_number on clicks: populated when fan identity is known (personalized links)
        "ALTER TABLE tracked_link_clicks ADD COLUMN IF NOT EXISTS phone_number TEXT DEFAULT NULL",
        "CREATE INDEX IF NOT EXISTS idx_tlc_phone ON tracked_link_clicks(phone_number) WHERE phone_number IS NOT NULL",
        # msg_source on messages: 'bot' for reply messages, 'blast' for mass-send messages
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS msg_source TEXT DEFAULT 'bot'",
        # sent_to on tracked_links: cumulative recipients across all blasts using this link
        "ALTER TABLE tracked_links ADD COLUMN IF NOT EXISTS sent_to INT DEFAULT 0",
        "ALTER TABLE operator_blast_images ADD COLUMN IF NOT EXISTS data_b64 TEXT",

        # Drop NOT NULL on legacy BYTEA column — only needed on old deployments
        # that created the table before we switched to data_b64.
        # Silently skipped on fresh DBs where the column is already nullable.
        "ALTER TABLE operator_blast_images ALTER COLUMN data DROP NOT NULL",

        # ── Quiz tables (shared with main app via same DATABASE_URL) ──────────
        """
        CREATE TABLE IF NOT EXISTS quiz_sessions (
            id             SERIAL PRIMARY KEY,
            show_id        INT,
            blast_draft_id BIGINT,
            question_text  TEXT NOT NULL,
            correct_answer TEXT NOT NULL,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            expires_at     TIMESTAMPTZ
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS quiz_responses (
            id           BIGSERIAL PRIMARY KEY,
            quiz_id      INT  NOT NULL REFERENCES quiz_sessions(id) ON DELETE CASCADE,
            phone_number TEXT NOT NULL,
            fan_answer   TEXT NOT NULL DEFAULT '',
            answered_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (quiz_id, phone_number)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_quiz_sessions_active ON quiz_sessions (expires_at, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_quiz_responses_lookup ON quiz_responses (quiz_id, phone_number)",

        # ── Quiz columns on blast_drafts ───────────────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS is_quiz BOOLEAN DEFAULT FALSE",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS quiz_correct_answer TEXT DEFAULT ''",

        # ── Blast context columns + session table ─────────────────────────
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS blast_context_note TEXT DEFAULT ''",
        """
        CREATE TABLE IF NOT EXISTS blast_context_sessions (
            id             BIGSERIAL PRIMARY KEY,
            blast_draft_id BIGINT,
            context_note   TEXT NOT NULL,
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            expires_at     TIMESTAMPTZ
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_blast_context_sessions_active ON blast_context_sessions (expires_at, created_at)",

        # ── Analytics columns on blast_drafts ──────────────────────────────
        # started_at: recorded before the send loop begins so reply attribution
        # uses the blast START time, not the end time (sent_at).
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS opt_out_count INT DEFAULT 0",
        # manual_link_clicks: for external/SlickText blasts where we can't count from our DB
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS manual_link_clicks INT DEFAULT NULL",
        # blast_category: 'friendly' | 'sales' | 'show' — used to split Blast Performance table
        "ALTER TABLE blast_drafts ADD COLUMN IF NOT EXISTS blast_category TEXT DEFAULT NULL",

        # ── blast_recipients: per-fan record of every blast send ────────────
        # Foundation for Smart Blast frequency logic and per-fan tier cadence.
        """
        CREATE TABLE IF NOT EXISTS blast_recipients (
            id           BIGSERIAL PRIMARY KEY,
            blast_id     BIGINT NOT NULL REFERENCES blast_drafts(id) ON DELETE CASCADE,
            phone_number TEXT NOT NULL,
            sent_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (blast_id, phone_number)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_blast_recipients_phone ON blast_recipients (phone_number, sent_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_blast_recipients_blast ON blast_recipients (blast_id)",

        # ── Blast image access tokens ──────────────────────────────────────
        # access_token: random hex included in the image URL so sequential IDs
        # can't be enumerated to fetch other tenants' uploaded images.
        # Existing rows get a deterministic token derived from their id so old
        # Twilio MMS URLs stay valid after the migration.
        "ALTER TABLE operator_blast_images ADD COLUMN IF NOT EXISTS access_token TEXT DEFAULT NULL",
        """
        UPDATE operator_blast_images
        SET access_token = md5(id::text || 'zar-img-salt')
        WHERE access_token IS NULL
        """,

        # ── Password reset tokens ──────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id         BIGSERIAL PRIMARY KEY,
            user_id    BIGINT NOT NULL REFERENCES operator_users(id) ON DELETE CASCADE,
            token      TEXT UNIQUE NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            used_at    TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_prt_token ON password_reset_tokens(token)",
        "CREATE INDEX IF NOT EXISTS idx_prt_user ON password_reset_tokens(user_id)",

        # ── Onboarding / self-serve tables ─────────────────────────────────

        # Columns added to operator_users for self-serve accounts
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS creator_slug TEXT DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS account_type TEXT DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS is_super_admin BOOLEAN DEFAULT FALSE",

        # bot_configs: one row per self-signed-up creator; written by /api/onboarding/submit
        """
        CREATE TABLE IF NOT EXISTS bot_configs (
            id               BIGSERIAL PRIMARY KEY,
            operator_user_id BIGINT NOT NULL REFERENCES operator_users(id) ON DELETE CASCADE,
            creator_slug     TEXT UNIQUE NOT NULL,
            account_type     TEXT NOT NULL DEFAULT 'performer',
            config_json      JSONB NOT NULL DEFAULT '{}',
            status           TEXT NOT NULL DEFAULT 'submitted',
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_bot_configs_user ON bot_configs(operator_user_id)",

        # smb_bot_config: DB-persisted overrides for business bot settings
        """
        CREATE TABLE IF NOT EXISTS smb_bot_config (
            tenant_slug  TEXT PRIMARY KEY,
            config_json  JSONB NOT NULL DEFAULT '{}',
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """,

        # operator_invites: pending team invites created by owners/admins
        """
        CREATE TABLE IF NOT EXISTS operator_invites (
            id           BIGSERIAL PRIMARY KEY,
            email        TEXT NOT NULL,
            creator_slug TEXT NOT NULL,
            account_type TEXT NOT NULL DEFAULT 'performer',
            invited_by   BIGINT REFERENCES operator_users(id) ON DELETE SET NULL,
            accepted_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (email, creator_slug)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_operator_invites_email ON operator_invites(email)",

        # ── Fan of the Week ────────────────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS fan_of_the_week (
            id           BIGSERIAL PRIMARY KEY,
            phone_number TEXT        NOT NULL,
            week_of      DATE        NOT NULL,
            message_text TEXT        DEFAULT '',
            selected_at  TIMESTAMPTZ DEFAULT NOW(),
            creator_slug TEXT        NOT NULL DEFAULT 'zarna',
            UNIQUE (creator_slug, week_of)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_fotw_week ON fan_of_the_week (week_of DESC)",
        "CREATE INDEX IF NOT EXISTS idx_fotw_phone ON fan_of_the_week (phone_number)",

        # ── Multi-tenant backfill: add creator_slug to originally single-tenant tables ──
        # contacts, messages, and fan_of_the_week were built for Zarna only.
        # Default existing rows to 'zarna'; new rows get the slug from the bot pipeline.
        "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS creator_slug TEXT DEFAULT 'zarna'",
        "ALTER TABLE messages ADD COLUMN IF NOT EXISTS creator_slug TEXT DEFAULT 'zarna'",
        "ALTER TABLE fan_of_the_week ADD COLUMN IF NOT EXISTS creator_slug TEXT NOT NULL DEFAULT 'zarna'",
        # Migrate fan_of_the_week unique constraint from (week_of) → (creator_slug, week_of)
        # Idempotent: DROP CONSTRAINT errors are swallowed by the try/except in init_db.
        "ALTER TABLE fan_of_the_week DROP CONSTRAINT IF EXISTS fan_of_the_week_week_of_key",
        """DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fan_of_the_week_creator_slug_week_of_key'
            ) THEN
                ALTER TABLE fan_of_the_week
                  ADD CONSTRAINT fan_of_the_week_creator_slug_week_of_key
                  UNIQUE (creator_slug, week_of);
            END IF;
        END $$""",
        "CREATE INDEX IF NOT EXISTS idx_contacts_slug ON contacts (creator_slug)",
        "CREATE INDEX IF NOT EXISTS idx_messages_slug ON messages (creator_slug, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_fotw_slug_week ON fan_of_the_week (creator_slug, week_of DESC)",

        # ── Customer of the Week (SMB) ─────────────────────────────────────
        """
        CREATE TABLE IF NOT EXISTS smb_customer_of_the_week (
            id           BIGSERIAL PRIMARY KEY,
            tenant_slug  TEXT        NOT NULL,
            phone_number TEXT        NOT NULL,
            week_of      DATE        NOT NULL,
            message_text TEXT        DEFAULT '',
            selected_at  TIMESTAMPTZ DEFAULT NOW(),
            shows_attended INT       DEFAULT 0,
            UNIQUE (tenant_slug, week_of)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cotw_tenant_week ON smb_customer_of_the_week (tenant_slug, week_of DESC)",
        "CREATE INDEX IF NOT EXISTS idx_cotw_phone ON smb_customer_of_the_week (phone_number)",

        # ── Universal Bot Pipeline: personality configs + RAG embeddings ───
        # creator_configs: personality JSON per creator. Lives in Postgres (not
        # disk) so it survives Railway redeploys. Replaces the old pattern of
        # writing creator_config/<slug>.json files at provisioning time.
        """
        CREATE TABLE IF NOT EXISTS creator_configs (
            id           BIGSERIAL PRIMARY KEY,
            creator_slug TEXT UNIQUE NOT NULL,
            config_json  JSONB NOT NULL DEFAULT '{}',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cc_slug ON creator_configs(creator_slug)",

        # creator_embeddings: multi-tenant RAG chunks. Scoped by creator_slug —
        # every retrieval query filters WHERE creator_slug = %s, making cross-
        # creator leakage impossible. Requires pgvector extension (enable via
        # `CREATE EXTENSION IF NOT EXISTS vector;` in Railway Postgres console).
        #
        # Dimension = 3072 to match gemini-embedding-001 (the model used by
        # scripts/build_embeddings.py and already baked into training_data/).
        # The HNSW halfvec index supports up to 4000 dimensions — ivfflat does
        # not work above ~2000, which is why we cast to halfvec for the index.
        """
        CREATE TABLE IF NOT EXISTS creator_embeddings (
            id           BIGSERIAL PRIMARY KEY,
            creator_slug TEXT NOT NULL,
            chunk_text   TEXT NOT NULL,
            source       TEXT NOT NULL DEFAULT 'general',
            embedding    vector(3072),
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ce_slug ON creator_embeddings(creator_slug)",
        """
        CREATE INDEX IF NOT EXISTS idx_ce_embedding_hnsw
            ON creator_embeddings
            USING hnsw ((embedding::halfvec(3072)) halfvec_cosine_ops)
        """,

        # Provisioning status tracking on bot_configs — surfaces pipeline
        # failures to the frontend (e.g. scraper broke, Gemini rate-limited).
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS error_message TEXT",
        "ALTER TABLE bot_configs ADD COLUMN IF NOT EXISTS provisioning_status TEXT DEFAULT NULL",

        # ── Billing / Stripe / Credits ─────────────────────────────────────
        # plan_tier: 'trial' for new signups; set to paid tier name by Stripe webhook.
        # trial_credits_remaining: only used while plan_tier='trial' — hard stopped at 0.
        # Paid plans get credits_included from operator_credit_usage instead.
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS plan_tier TEXT DEFAULT 'trial'",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS billing_cycle TEXT DEFAULT 'monthly'",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS stripe_subscription_id TEXT DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS billing_cycle_anchor TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS trial_credits_remaining INT DEFAULT 1000",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS trial_started_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS sent_trial_low_alert BOOLEAN DEFAULT FALSE",
        "ALTER TABLE operator_users ADD COLUMN IF NOT EXISTS sent_trial_exhausted_alert BOOLEAN DEFAULT FALSE",
        "CREATE INDEX IF NOT EXISTS idx_operator_users_stripe_customer ON operator_users(stripe_customer_id) WHERE stripe_customer_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_operator_users_stripe_subscription ON operator_users(stripe_subscription_id) WHERE stripe_subscription_id IS NOT NULL",

        # operator_credit_usage: monthly roll-up of credits consumed per user.
        # Primary read path for /api/billing/status — kept separate from
        # credit_events so the status endpoint doesn't aggregate thousands of
        # rows on every page load.
        """
        CREATE TABLE IF NOT EXISTS operator_credit_usage (
            id                 BIGSERIAL PRIMARY KEY,
            operator_user_id   BIGINT NOT NULL REFERENCES operator_users(id) ON DELETE CASCADE,
            creator_slug       TEXT NOT NULL,
            period_start       DATE NOT NULL,
            period_end         DATE,
            credits_used       INT NOT NULL DEFAULT 0,
            credits_included   INT NOT NULL DEFAULT 0,
            boosters_purchased INT NOT NULL DEFAULT 0,
            overage_credits    INT NOT NULL DEFAULT 0,
            updated_at         TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (operator_user_id, period_start)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ocu_user ON operator_credit_usage(operator_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_ocu_slug ON operator_credit_usage(creator_slug)",

        # credit_events: granular audit log of every credit grant/consumption.
        # Enables per-user reporting ("where did my credits go?") and Stripe
        # reconciliation (match invoice to plan_reset event).
        """
        CREATE TABLE IF NOT EXISTS credit_events (
            id               BIGSERIAL PRIMARY KEY,
            operator_user_id BIGINT NOT NULL REFERENCES operator_users(id) ON DELETE CASCADE,
            creator_slug     TEXT NOT NULL,
            kind             TEXT NOT NULL,
            credits          INT NOT NULL,
            source_id        TEXT DEFAULT NULL,
            created_at       TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_credit_events_user ON credit_events(operator_user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_credit_events_slug ON credit_events(creator_slug, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_credit_events_kind ON credit_events(kind)",

        # ── Team members (dedicated table, replaces ad-hoc creator_slug sharing) ──
        # tenant_slug = the owner's creator_slug. Multiple operator_users rows
        # can belong to the same tenant. Enforced seat limit by plan_tier.
        """
        CREATE TABLE IF NOT EXISTS team_members (
            id           BIGSERIAL PRIMARY KEY,
            tenant_slug  TEXT NOT NULL,
            user_id      BIGINT NOT NULL REFERENCES operator_users(id) ON DELETE CASCADE,
            role         TEXT NOT NULL DEFAULT 'member',
            invited_at   TIMESTAMPTZ DEFAULT NOW(),
            accepted_at  TIMESTAMPTZ,
            UNIQUE (tenant_slug, user_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_team_members_tenant ON team_members(tenant_slug)",
        "CREATE INDEX IF NOT EXISTS idx_team_members_user ON team_members(user_id)",

        # Backfill: every existing operator_users row with a creator_slug becomes
        # a team_members row. Owner is the user whose id matches the slug's
        # bot_configs.operator_user_id; everyone else with the same slug is a
        # member. INSERT ... ON CONFLICT makes this idempotent.
        """
        INSERT INTO team_members (tenant_slug, user_id, role, invited_at, accepted_at)
        SELECT u.creator_slug, u.id,
               CASE WHEN u.id = bc.operator_user_id THEN 'owner' ELSE 'member' END,
               u.created_at,
               u.created_at
        FROM   operator_users u
        LEFT JOIN bot_configs bc ON bc.creator_slug = u.creator_slug
        WHERE  u.creator_slug IS NOT NULL
          AND  u.creator_slug <> ''
        ON CONFLICT (tenant_slug, user_id) DO NOTHING
        """,

        # ── Grandfathered / founder accounts ───────────────────────────────
        # These slugs pre-date Stripe and are explicitly granted unlimited
        # usage by the product owner. Re-running is safe (ON CONFLICT). If
        # the account isn't onboarded yet, the UPDATE matches zero rows.
        # Can be extended by editing this list — the app reads plan_tier
        # every request, so changes take effect on the next call.
        """
        UPDATE operator_users
        SET    plan_tier = 'grandfathered',
               trial_credits_remaining = 0
        WHERE  creator_slug IN ('zarna', 'west_side_comedy')
          AND  plan_tier <> 'grandfathered'
        """,

        # ── Smart Send: engagement score on contacts ──────────────────────
        # Recomputed nightly based on reply recency, session depth, click activity.
        # Used by /api/contacts/engaged?top=N for the Smart Send audience selector.
        "ALTER TABLE contacts ADD COLUMN IF NOT EXISTS engagement_score INT DEFAULT 0",
        "CREATE INDEX IF NOT EXISTS idx_contacts_engagement ON contacts(engagement_score DESC) WHERE engagement_score > 0",

        # ── Multi-tenant ownership on live_shows ──────────────────────────
        # Prior releases referenced live_shows.created_by / creator_slug from
        # the _user_owns_show() guard but never actually added the columns,
        # which made every activate/end/delete API call 404 for non-super
        # admins. We add both (idempotent) and backfill historical rows with
        # Zarna's slug/owner email since every existing show pre-dates
        # multi-tenancy.
        "ALTER TABLE live_shows ADD COLUMN IF NOT EXISTS creator_slug TEXT DEFAULT NULL",
        "ALTER TABLE live_shows ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT NULL",
        "UPDATE live_shows SET creator_slug='zarna' WHERE creator_slug IS NULL OR creator_slug=''",
        "UPDATE live_shows SET created_by='brij@zarnagarg.com' WHERE created_by IS NULL OR created_by=''",
        "CREATE INDEX IF NOT EXISTS idx_live_shows_creator_slug ON live_shows(creator_slug)",

        # ── Data-cleanup on every startup ──────────────────────────────────
        # 1. Clear /tmp-based image URLs (ephemeral Railway filesystem, gone on redeploy)
        "UPDATE blast_drafts SET media_url='' WHERE media_url LIKE '%/operator/blast/uploads/%'",

        # 2. Clear any DB image URLs that reference a row with no valid data_b64
        #    (these came from failed uploads before the NOT NULL fix landed).
        #    We use a substring trick: extract the numeric ID from the URL path
        #    /operator/blast/img/<id>/<filename> and cross-check the images table.
        """
        UPDATE blast_drafts
        SET    media_url = ''
        WHERE  media_url LIKE '%/operator/blast/img/%'
          AND  NOT EXISTS (
                  SELECT 1
                  FROM   operator_blast_images oi
                  WHERE  blast_drafts.media_url LIKE '%/operator/blast/img/' || oi.id || '/%'
                    AND  oi.data_b64 IS NOT NULL
                    AND  LENGTH(oi.data_b64) > 0
               )
        """,
    ]

    conn = get_conn()
    try:
        conn.autocommit = True          # each statement is its own implicit txn
        with conn.cursor() as cur:
            for stmt in statements:
                label = stmt.strip()[:80].replace("\n", " ")
                try:
                    cur.execute(stmt)
                    rows = cur.rowcount if cur.rowcount >= 0 else 0
                    log.info("init_db OK  [rows=%d]: %s", rows, label)
                except Exception as e:
                    # Expected on fresh DBs (e.g. "column already not-null").
                    # Log as warning so it's visible in Railway deploy logs.
                    log.warning("init_db SKIP: %s — %s", label, e)
    finally:
        conn.close()
