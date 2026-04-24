"""
Stripe checkout + portal + webhook endpoints.

Routes:
  POST /api/billing/create-checkout-session   start a Subscription or Booster Checkout
  POST /api/billing/portal                    return a Stripe Customer Portal URL
  POST /api/billing/webhook                   Stripe → us webhook (raw body + sig verified)

Env required:
  STRIPE_SECRET_KEY
  STRIPE_WEBHOOK_SECRET
  STRIPE_PUBLISHABLE_KEY      (exposed to frontend via VITE_STRIPE_PUBLISHABLE_KEY)
  STRIPE_PRICE_ID_*           (per-plan + per-booster)
  FRONTEND_URL                where to redirect after checkout success/cancel
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from flask import Blueprint, jsonify, request

from ..db import get_conn
from ..routes.auth import login_required, current_user
from ..billing.credits import (
    clear_subscription,
    grant_booster_credits,
    set_plan_tier,
)
from ..billing.plans import (
    ALL_PLANS,
    BOOSTERS,
    booster_by_price_id,
    booster_price_id,
    plan_by_price_id,
    stripe_price_id,
)

logger = logging.getLogger(__name__)

billing_bp = Blueprint("billing", __name__)


def _stripe():
    """Lazy-load the Stripe SDK so the module imports cleanly on dev machines
    that haven't installed the dependency yet. Raises at call time only when
    a route is actually invoked.
    """
    import stripe  # type: ignore
    key = os.getenv("STRIPE_SECRET_KEY", "").strip()
    if not key:
        raise RuntimeError("STRIPE_SECRET_KEY is not set")
    stripe.api_key = key
    return stripe


def _frontend_url() -> str:
    return os.getenv("FRONTEND_URL", "http://localhost:8080").rstrip("/")


def _ensure_stripe_customer(user: dict) -> str:
    """Return an existing stripe_customer_id for this user, or create one.

    Stripe customers are shared across all products/subs for one user so the
    Customer Portal can display every invoice and subscription together.
    """
    existing = (user.get("stripe_customer_id") or "").strip()
    if existing:
        return existing

    stripe = _stripe()
    customer = stripe.Customer.create(
        email=user["email"],
        name=user.get("name") or None,
        metadata={
            "operator_user_id": str(user["id"]),
            "creator_slug": user.get("creator_slug") or "",
        },
    )
    cust_id = customer["id"]

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE operator_users SET stripe_customer_id=%s WHERE id=%s",
                    (cust_id, user["id"]),
                )
    finally:
        conn.close()
    logger.info("stripe_customer created id=%s for user_id=%s", cust_id, user["id"])
    return cust_id


# ── GET /api/billing/plans ────────────────────────────────────────────────

@billing_bp.route("/api/billing/plans", methods=["GET"])
def list_plans():
    """Public endpoint — backs the /plans page.

    Returns the static plan + booster catalog plus which prices are actually
    configured in the current Stripe environment (so the UI can hide/disable
    plans that haven't been wired up yet).
    """
    def _plan_json(plan):
        return {
            "tier": plan.tier,
            "label": plan.label,
            "audience": plan.audience,
            "monthly_credits": plan.monthly_credits,
            "monthly_price_usd": plan.monthly_price_usd,
            "annual_price_usd": plan.annual_price_usd,
            "seats": plan.seats,
            "available_monthly": bool(stripe_price_id(plan.tier, "monthly")),
            "available_annual": bool(stripe_price_id(plan.tier, "annual")),
        }

    def _booster_json(b):
        return {
            "key": b.key,
            "label": b.label,
            "credits": b.credits,
            "price_usd": b.price_usd,
            "available": bool(booster_price_id(b.key)),
        }

    return jsonify(
        plans=[_plan_json(p) for p in ALL_PLANS.values()],
        boosters=[_booster_json(b) for b in BOOSTERS.values()],
    )


# ── POST /api/billing/create-checkout-session ─────────────────────────────

@billing_bp.route("/api/billing/create-checkout-session", methods=["POST"])
@login_required
def create_checkout_session():
    """Start a Stripe Checkout Session.

    Body (one of):
        { "plan_tier": "starter", "billing_cycle": "monthly" }
        { "booster": "mini" }

    Returns { url } — client does window.location = url.
    """
    data = request.get_json(silent=True) or {}
    plan_tier = (data.get("plan_tier") or "").strip()
    billing_cycle = (data.get("billing_cycle") or "monthly").strip()
    booster_key = (data.get("booster") or "").strip()

    user = current_user()
    # Fresh row — need stripe_customer_id
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT id, email, name, creator_slug, stripe_customer_id
                   FROM operator_users WHERE id=%s""",
                (user["id"],),
            )
            row = cur.fetchone()
            fresh = dict(row) if row else dict(user)
    finally:
        conn.close()

    try:
        stripe = _stripe()
    except RuntimeError as e:
        logger.error("create_checkout_session: %s", e)
        return jsonify(error="Billing not configured yet. Please contact support."), 503

    try:
        customer_id = _ensure_stripe_customer(fresh)
    except Exception:
        logger.exception("create_checkout_session: could not create Stripe customer")
        return jsonify(error="Could not start checkout."), 500

    front = _frontend_url()

    # ── Booster (one-time purchase) ─────────────────────────────────────
    if booster_key:
        booster = BOOSTERS.get(booster_key)
        if not booster:
            return jsonify(error=f"Unknown booster: {booster_key}"), 400
        price_id = booster_price_id(booster_key)
        if not price_id:
            return jsonify(error="This booster isn't available yet."), 503

        try:
            session = stripe.checkout.Session.create(
                mode="payment",
                customer=customer_id,
                line_items=[{"price": price_id, "quantity": 1}],
                success_url=f"{front}/billing?booster=success",
                cancel_url=f"{front}/billing?booster=cancelled",
                metadata={
                    "kind": "booster",
                    "booster": booster_key,
                    "credits": str(booster.credits),
                    "operator_user_id": str(fresh["id"]),
                    "creator_slug": fresh.get("creator_slug") or "",
                },
                allow_promotion_codes=True,
            )
            return jsonify(url=session["url"])
        except Exception:
            logger.exception("create_checkout_session: booster failed booster=%s", booster_key)
            return jsonify(error="Checkout failed — please try again."), 500

    # ── Subscription (recurring) ────────────────────────────────────────
    if plan_tier not in ALL_PLANS:
        return jsonify(error=f"Unknown plan: {plan_tier}"), 400
    if billing_cycle not in ("monthly", "annual"):
        billing_cycle = "monthly"

    price_id = stripe_price_id(plan_tier, billing_cycle)
    if not price_id:
        return jsonify(error=f"{plan_tier} plan isn't configured yet. Contact support."), 503

    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            customer=customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=f"{front}/billing?checkout=success",
            cancel_url=f"{front}/plans?checkout=cancelled",
            metadata={
                "kind": "subscription",
                "plan_tier": plan_tier,
                "billing_cycle": billing_cycle,
                "operator_user_id": str(fresh["id"]),
                "creator_slug": fresh.get("creator_slug") or "",
            },
            subscription_data={
                "metadata": {
                    "plan_tier": plan_tier,
                    "billing_cycle": billing_cycle,
                    "operator_user_id": str(fresh["id"]),
                    "creator_slug": fresh.get("creator_slug") or "",
                },
            },
            allow_promotion_codes=True,
        )
        return jsonify(url=session["url"])
    except Exception:
        logger.exception("create_checkout_session: subscription failed tier=%s cycle=%s",
                         plan_tier, billing_cycle)
        return jsonify(error="Checkout failed — please try again."), 500


# ── POST /api/billing/portal ───────────────────────────────────────────────

@billing_bp.route("/api/billing/portal", methods=["POST"])
@login_required
def create_portal_session():
    """Return a Stripe Customer Portal URL for the current user."""
    user = current_user()
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, email, name, creator_slug, stripe_customer_id FROM operator_users WHERE id=%s",
                (user["id"],),
            )
            row = cur.fetchone()
            fresh = dict(row) if row else dict(user)
    finally:
        conn.close()

    try:
        stripe = _stripe()
        customer_id = _ensure_stripe_customer(fresh)
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{_frontend_url()}/billing",
        )
        return jsonify(url=session["url"])
    except Exception:
        logger.exception("create_portal_session failed for user_id=%s", user["id"])
        return jsonify(error="Could not open portal."), 500


# ── POST /api/billing/webhook ──────────────────────────────────────────────

@billing_bp.route("/api/billing/webhook", methods=["POST"])
def stripe_webhook():
    """Handle Stripe webhook events.

    Signature verified via STRIPE_WEBHOOK_SECRET. This route must be exempt
    from the app-wide CSRF check (see operator/app/__init__.py — we add
    `/api/billing/webhook` to _CSRF_EXEMPT_PATHS).
    """
    secret = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
    sig_header = request.headers.get("Stripe-Signature", "")
    payload = request.get_data()

    try:
        stripe = _stripe()
    except RuntimeError:
        logger.error("stripe_webhook: STRIPE_SECRET_KEY missing")
        return jsonify(error="Billing not configured."), 503

    if not secret:
        logger.error("stripe_webhook: STRIPE_WEBHOOK_SECRET missing")
        return jsonify(error="Webhook secret not configured."), 503

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, secret)
    except Exception:
        logger.warning("stripe_webhook: signature verification failed", exc_info=True)
        return jsonify(error="Invalid signature"), 400

    etype = event["type"]
    logger.info("stripe_webhook: received %s id=%s", etype, event.get("id"))

    try:
        if etype == "checkout.session.completed":
            _handle_checkout_completed(event["data"]["object"])
        elif etype == "invoice.paid":
            _handle_invoice_paid(event["data"]["object"])
        elif etype == "customer.subscription.updated":
            _handle_subscription_updated(event["data"]["object"])
        elif etype == "customer.subscription.deleted":
            _handle_subscription_deleted(event["data"]["object"])
        else:
            logger.info("stripe_webhook: ignoring event type %s", etype)
    except Exception:
        logger.exception("stripe_webhook: handler failed for %s", etype)
        return jsonify(error="handler failed"), 500

    return jsonify(received=True)


# ── Webhook handlers ───────────────────────────────────────────────────────

def _lookup_user_by_customer(customer_id: str) -> Optional[dict]:
    conn = get_conn()
    try:
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, creator_slug FROM operator_users WHERE stripe_customer_id=%s",
                (customer_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _handle_checkout_completed(session: dict) -> None:
    """First-time Checkout success.

    For mode='subscription', the subscription webhook will follow immediately
    with the real period anchor — so here we just need to connect the customer
    to the operator_user (idempotent upsert) and grant any immediate credits
    for boosters purchased via mode='payment'.
    """
    meta = session.get("metadata") or {}
    kind = meta.get("kind") or ""

    if kind == "booster":
        uid_str = meta.get("operator_user_id") or ""
        slug = meta.get("creator_slug") or ""
        credits = int(meta.get("credits") or "0")
        if uid_str and credits > 0:
            grant_booster_credits(
                user_id=int(uid_str),
                slug=slug,
                credits=credits,
                stripe_invoice_id=session.get("payment_intent") or session.get("id"),
            )
        return

    if kind == "subscription":
        uid_str = meta.get("operator_user_id") or ""
        slug = meta.get("creator_slug") or ""
        plan_tier = meta.get("plan_tier") or ""
        billing_cycle = meta.get("billing_cycle") or "monthly"
        sub_id = session.get("subscription") or ""
        customer_id = session.get("customer") or ""

        plan = ALL_PLANS.get(plan_tier)
        if not uid_str or not plan:
            logger.warning("checkout_completed: missing meta (uid=%s plan=%s)", uid_str, plan_tier)
            return

        set_plan_tier(
            user_id=int(uid_str),
            slug=slug,
            plan_tier=plan_tier,
            billing_cycle=billing_cycle,
            stripe_customer_id=customer_id,
            stripe_subscription_id=sub_id,
            billing_cycle_anchor=datetime.now(timezone.utc),
            included_credits=plan.monthly_credits,
        )


def _handle_invoice_paid(invoice: dict) -> None:
    """Every recurring renewal.

    Rolls the billing period: resets credits_used to 0, sets new period_end,
    logs a credit_events row for auditability.
    """
    sub_id = invoice.get("subscription")
    customer_id = invoice.get("customer")
    if not sub_id or not customer_id:
        return

    user = _lookup_user_by_customer(customer_id)
    if not user:
        logger.warning("invoice_paid: no user for customer=%s", customer_id)
        return

    # Pull the subscription to get the canonical current_period_start
    try:
        stripe = _stripe()
        sub = stripe.Subscription.retrieve(sub_id)
    except Exception:
        logger.exception("invoice_paid: could not fetch subscription %s", sub_id)
        return

    meta = sub.get("metadata") or {}
    plan_tier = meta.get("plan_tier") or ""
    billing_cycle = meta.get("billing_cycle") or "monthly"
    plan = ALL_PLANS.get(plan_tier)
    # Fall back to reverse price-id lookup if metadata is missing
    if not plan:
        items = (sub.get("items") or {}).get("data") or []
        if items:
            price_id = items[0].get("price", {}).get("id", "")
            found = plan_by_price_id(price_id)
            if found:
                plan, billing_cycle = found
                plan_tier = plan.tier

    if not plan:
        logger.warning("invoice_paid: cannot resolve plan for sub=%s", sub_id)
        return

    anchor_ts = sub.get("current_period_start")
    anchor = (
        datetime.fromtimestamp(anchor_ts, tz=timezone.utc)
        if anchor_ts else datetime.now(timezone.utc)
    )

    set_plan_tier(
        user_id=user["id"],
        slug=user.get("creator_slug") or "",
        plan_tier=plan_tier,
        billing_cycle=billing_cycle,
        stripe_customer_id=customer_id,
        stripe_subscription_id=sub_id,
        billing_cycle_anchor=anchor,
        included_credits=plan.monthly_credits,
    )


def _handle_subscription_updated(sub: dict) -> None:
    """Tier change mid-cycle (plan upgrade/downgrade)."""
    customer_id = sub.get("customer")
    if not customer_id:
        return
    user = _lookup_user_by_customer(customer_id)
    if not user:
        return

    meta = sub.get("metadata") or {}
    plan_tier = meta.get("plan_tier") or ""
    billing_cycle = meta.get("billing_cycle") or "monthly"
    plan = ALL_PLANS.get(plan_tier)

    if not plan:
        items = (sub.get("items") or {}).get("data") or []
        if items:
            price_id = items[0].get("price", {}).get("id", "")
            found = plan_by_price_id(price_id)
            if found:
                plan, billing_cycle = found
                plan_tier = plan.tier

    if not plan:
        return

    anchor_ts = sub.get("current_period_start")
    anchor = (
        datetime.fromtimestamp(anchor_ts, tz=timezone.utc)
        if anchor_ts else datetime.now(timezone.utc)
    )

    set_plan_tier(
        user_id=user["id"],
        slug=user.get("creator_slug") or "",
        plan_tier=plan_tier,
        billing_cycle=billing_cycle,
        stripe_customer_id=customer_id,
        stripe_subscription_id=sub.get("id"),
        billing_cycle_anchor=anchor,
        included_credits=plan.monthly_credits,
    )


def _handle_subscription_deleted(sub: dict) -> None:
    customer_id = sub.get("customer")
    if not customer_id:
        return
    user = _lookup_user_by_customer(customer_id)
    if not user:
        return
    clear_subscription(user_id=user["id"])
