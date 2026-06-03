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

# A2P 10DLC compliance line auto-appended to a performer's first message to a
# new fan. Fan-initiated conversations don't need a "Reply YES" opt-in gate
# (the inbound text is consent), but the program must still disclose data rates
# and support STOP/HELP. This is shown greyed-out + non-editable in the My Bot
# UI so creators know it's always included. Mirrors the SMB footer in
# app/smb/onboarding.py.
PERFORMER_COMPLIANCE_FOOTER = os.getenv(
    "PERFORMER_COMPLIANCE_FOOTER",
    "Msg & data rates may apply. Reply STOP to opt out, HELP for help.",
)
