# WSCC Launch Verification Checklist

A short list of operational checks to run against production before pointing the West Side Comedy Club owner at the dashboard.
None of these are code changes; they are SQL / shell verifications that the deploy is in the expected state for a grandfathered unlimited business account.

---

## 1. WSCC plan_tier is grandfathered (Unlimited)

The Billing UI now strictly trusts the API's `unlimited` flag (we removed the
"`credits_total === 0` ⇒ Unlimited" fallback). That means WSCC's owner row in
`operator_users` MUST report a tier the billing layer treats as unlimited.

```sql
SELECT email, creator_slug, plan_tier, stripe_customer_id, trial_credits_remaining
FROM   operator_users
WHERE  creator_slug = 'wscc'
ORDER  BY id;
```

Expected: at least one row with `plan_tier IN ('grandfathered', 'founder', 'internal')`
(see `app/billing/plans.py::is_unlimited_tier`). If the row is still on
`'trial'`, run:

```sql
UPDATE operator_users
SET    plan_tier = 'grandfathered'
WHERE  creator_slug = 'wscc' AND email = '<owner_email>';
```

Then refresh the dashboard — Billing should show the **Unlimited** badge and
hide the "Buy credits" / upsell sections.

---

## 2. SMB engagement schema is up to date

Smart Send for businesses depends on the new `smb_blast_recipients` table plus
the engagement columns that classify subscribers into Regular / Engaged / New /
Lapsed tiers. The migration is idempotent and runs at app boot via
`init_db()`, but if the deploy's startup logs were noisy this is worth
verifying explicitly.

```sql
\d smb_blast_recipients
\d smb_subscribers
```

`smb_blast_recipients` should exist with `(blast_id, tenant_slug, phone_number,
sent_at, status)`. If it's missing, run the boot path manually from a Python
shell:

```python
from app.storage.postgres import init_db
init_db()
```

For the engagement columns specifically (last_replied_at, message_count, etc.):

```python
from app.smb.storage import ensure_smb_engagement_schema
ensure_smb_engagement_schema()
```

Both are safe to re-run.

---

## 3. SMS number is provisioned and routed

```sql
SELECT phone_number, provisioning_status, twilio_phone_sid
FROM   operator_users
WHERE  creator_slug = 'wscc';
```

`provisioning_status` should be `'live'`. Cross-check that
`SMB_WSCC_SMS_NUMBER` is set in the Railway environment for **both** the
`main` and `operator` services — the inbox tab and the blast-send path both
read it.

---

## 4. Smoke test the new business blast composer

Log in as the WSCC owner (or use admin "view as wscc") and:

1. Open `/blasts` — the page should render the new business composer (lists
   past promos, "New Promo" button), NOT redirect to `/dashboard`.
2. Click "New Promo". The audience selector should show:
   - Smart Send (with per-tier breakdown counts > 0)
   - Everyone
   - Past Customers of the Week (if any picks exist)
   - By tier (Regular / Engaged / New / Lapsed)
   - By segment
3. Toggle "AI cleanup" on, type a quick message, hit Preview — recipient count
   should be > 0 for "Smart Send" if the suppression window is empty.

If the per-tier counts come back as 0 across the board, the engagement
classification CTE in `operator/app/business_blast.py` is reading from an empty
`smb_messages` table — confirm inbound messages are landing there:

```sql
SELECT COUNT(*) FROM smb_messages WHERE tenant_slug = 'wscc';
```

---

## 5. Performer-only API endpoints are blocked for business

The new `_require_performer_account()` guard returns **404** when a business
account hits a performer endpoint (dashboard/stats, audience, inbox, shows,
fan-of-the-week, blasts list). Verify:

```bash
# Logged in as the WSCC owner:
curl -i https://app.zar.bot/api/dashboard/stats   # expect 404
curl -i https://app.zar.bot/api/business/stats    # expect 200
```

Super-admins are exempt — admin "view as wscc" should still see all endpoints.
