"""
operator/app/billing — Stripe + credits subsystem.

Public surface:
- credits.consume_credit()          decrement + audit
- credits.get_credit_status()       read-only snapshot
- credits.enforce_send_quota()      soft-grace / hard-trial gate
- credits.seed_trial_credits()      called at end of onboarding
- plans.PLAN_CREDITS, PLAN_SEATS    static plan map
- stripe_client.get_stripe()        singleton stripe SDK instance
"""
