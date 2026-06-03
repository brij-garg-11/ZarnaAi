"""
Single source of truth for the platform brand name.

Everything user-facing that names the platform (emails, A2P opt-in copy,
admin UI) should read from here so a rename is a one-line change. The
public-site (Lovable) frontend has its own mirror of this constant.
"""

import os

# Display name shown to humans (emails, copy). Override per-env if needed.
PLATFORM_BRAND = os.getenv("PLATFORM_BRAND", "twowaybot")

# Lowercase machine/handle form (domains, message-ids, slugs).
PLATFORM_BRAND_SLUG = os.getenv(
    "PLATFORM_BRAND_SLUG", PLATFORM_BRAND.replace(" ", "").lower()
)
