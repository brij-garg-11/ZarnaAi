"""
Plan + booster catalog.

Kept in code (not DB) so plan details are versioned with the app and webhooks
can look up included credits synchronously. Price IDs come from Stripe dashboard
setup and are referenced by env var name — so the same code runs against test
and live keys by swapping env.

1 credit == 1 SMS segment sent or received.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Plan:
    tier: str
    label: str
    audience: str  # "performer" | "business"
    monthly_credits: int
    monthly_price_usd: int
    annual_price_usd: int
    seats: Optional[int]  # None = unlimited
    stripe_price_env_monthly: str
    stripe_price_env_annual: str


# Performer tiers (from docs/engineering/remaining_work.md)
PERFORMER_PLANS: dict[str, Plan] = {
    "starter": Plan(
        tier="starter", label="Starter", audience="performer",
        monthly_credits=3200, monthly_price_usd=79, annual_price_usd=790,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_STARTER_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_STARTER_ANNUAL",
    ),
    "growth": Plan(
        tier="growth", label="Growth", audience="performer",
        monthly_credits=6200, monthly_price_usd=149, annual_price_usd=1490,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_GROWTH_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_GROWTH_ANNUAL",
    ),
    "pro": Plan(
        tier="pro", label="Pro", audience="performer",
        monthly_credits=12500, monthly_price_usd=299, annual_price_usd=2990,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_PRO_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_PRO_ANNUAL",
    ),
    "scale": Plan(
        tier="scale", label="Scale", audience="performer",
        monthly_credits=25200, monthly_price_usd=599, annual_price_usd=5990,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_SCALE_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_SCALE_ANNUAL",
    ),
    "elite": Plan(
        tier="elite", label="Elite", audience="performer",
        monthly_credits=41900, monthly_price_usd=999, annual_price_usd=9990,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_ELITE_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_ELITE_ANNUAL",
    ),
    "creator": Plan(
        tier="creator", label="Creator", audience="performer",
        monthly_credits=80300, monthly_price_usd=1999, annual_price_usd=19990,
        seats=1,
        stripe_price_env_monthly="STRIPE_PRICE_ID_CREATOR_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_CREATOR_ANNUAL",
    ),
}

# Business tiers
BUSINESS_PLANS: dict[str, Plan] = {
    "essentials": Plan(
        tier="essentials", label="Essentials", audience="business",
        monthly_credits=1900, monthly_price_usd=49, annual_price_usd=490,
        seats=2,
        stripe_price_env_monthly="STRIPE_PRICE_ID_ESSENTIALS_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_ESSENTIALS_ANNUAL",
    ),
    "standard": Plan(
        tier="standard", label="Standard", audience="business",
        monthly_credits=4300, monthly_price_usd=99, annual_price_usd=990,
        seats=4,
        stripe_price_env_monthly="STRIPE_PRICE_ID_STANDARD_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_STANDARD_ANNUAL",
    ),
    "business_pro": Plan(
        tier="business_pro", label="Business Pro", audience="business",
        monthly_credits=9000, monthly_price_usd=199, annual_price_usd=1990,
        seats=None,  # Unlimited
        stripe_price_env_monthly="STRIPE_PRICE_ID_BUSINESS_PRO_MONTHLY",
        stripe_price_env_annual="STRIPE_PRICE_ID_BUSINESS_PRO_ANNUAL",
    ),
}

ALL_PLANS: dict[str, Plan] = {**PERFORMER_PLANS, **BUSINESS_PLANS}


# Free trial — every new signup
TRIAL_CREDITS = 1000


# Grandfathered / internal tiers that never hit the credit gate or show
# trial/upgrade UI. Used for founder accounts (Zarna) and early partner
# deployments (WSCC) that pre-date Stripe. Plans.py treats these as
# effectively unlimited — no credits_included row is required, no
# soft-grace math is done, no 402s are ever returned.
UNLIMITED_TIERS = frozenset({"grandfathered", "founder", "internal"})


def is_unlimited_tier(plan_tier: Optional[str]) -> bool:
    """True when the tier should bypass all credit gates."""
    return (plan_tier or "").lower() in UNLIMITED_TIERS


# One-time credit add-on packs
@dataclass(frozen=True)
class Booster:
    key: str
    label: str
    credits: int
    price_usd: int
    stripe_price_env: str


BOOSTERS: dict[str, Booster] = {
    "mini": Booster(
        key="mini", label="Mini boost",
        credits=500, price_usd=12,
        stripe_price_env="STRIPE_PRICE_ID_BOOSTER_MINI",
    ),
    "blast": Booster(
        key="blast", label="Blast pack",
        credits=1500, price_usd=32,
        stripe_price_env="STRIPE_PRICE_ID_BOOSTER_BLAST",
    ),
    "big_send": Booster(
        key="big_send", label="Big send",
        credits=4000, price_usd=79,
        stripe_price_env="STRIPE_PRICE_ID_BOOSTER_BIG_SEND",
    ),
    "power": Booster(
        key="power", label="Power pack",
        credits=10000, price_usd=179,
        stripe_price_env="STRIPE_PRICE_ID_BOOSTER_POWER",
    ),
}


# Soft-grace: allow up to SOFT_GRACE_MULTIPLIER × credits_total before hard block
SOFT_GRACE_MULTIPLIER = 1.10


def get_plan(tier: str) -> Optional[Plan]:
    return ALL_PLANS.get(tier)


def get_plan_credits(tier: str) -> int:
    """Monthly credits included in a given plan tier. Trial returns 1000."""
    if tier == "trial":
        return TRIAL_CREDITS
    plan = ALL_PLANS.get(tier)
    return plan.monthly_credits if plan else 0


def get_plan_seats(tier: str) -> Optional[int]:
    """Seat limit by plan. None = unlimited. Trial = 1."""
    if tier == "trial":
        return 1
    plan = ALL_PLANS.get(tier)
    return plan.seats if plan else 1


def stripe_price_id(tier: str, cycle: str) -> Optional[str]:
    """Resolve env var to a real Stripe Price ID. None when env isn't set."""
    plan = ALL_PLANS.get(tier)
    if not plan:
        return None
    env = plan.stripe_price_env_annual if cycle == "annual" else plan.stripe_price_env_monthly
    val = os.getenv(env, "").strip()
    return val or None


def booster_price_id(key: str) -> Optional[str]:
    b = BOOSTERS.get(key)
    if not b:
        return None
    val = os.getenv(b.stripe_price_env, "").strip()
    return val or None


def booster_by_price_id(price_id: str) -> Optional[Booster]:
    """Reverse lookup for Stripe webhook: which booster was just purchased?"""
    for b in BOOSTERS.values():
        if os.getenv(b.stripe_price_env, "").strip() == price_id:
            return b
    return None


def plan_by_price_id(price_id: str) -> Optional[tuple[Plan, str]]:
    """Reverse lookup: which plan + cycle maps to this Stripe Price ID?"""
    for plan in ALL_PLANS.values():
        for cycle, env in (
            ("monthly", plan.stripe_price_env_monthly),
            ("annual", plan.stripe_price_env_annual),
        ):
            if os.getenv(env, "").strip() == price_id:
                return plan, cycle
    return None
