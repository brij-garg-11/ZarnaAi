"""
operator/app/billing — Stripe + credits subsystem.

Public surface (verified against actual exports — keep in sync if the
modules are renamed/refactored):

- credits.consume_credit(user_id, slug, amount, source) — atomic decrement
  with `credit_events` audit row.
- credits.get_credit_status(user_id) — read-only snapshot of plan,
  included credits, used, remaining, billing period anchor.
- credits.check_send_quota(user_id) — pre-send gate used by the inbox
  send + blast worker; returns (ok: bool, reason: str | None).
- credits.seed_trial_credits(user_id, slug) — sets trial tier + credits at
  end of onboarding.
- credits.grant_booster_credits(user_id, slug, credits, stripe_invoice_id)
  — adds non-expiring booster credits in response to a Stripe checkout.
- plans.ALL_PLANS, plan_by_price_id, PERFORMER_PLANS, BUSINESS_PLANS,
  Plan dataclass — static plan catalog used by checkout/subscription
  handlers and the admin pricing UI.

There is no `enforce_send_quota()` or `stripe_client.get_stripe()` in this
package — historically referenced in this docstring but never implemented;
the corresponding callers are `check_send_quota()` and a private `_stripe()`
helper inside `routes/billing.py`.
"""
