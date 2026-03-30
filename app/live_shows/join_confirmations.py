"""Random SMS copy for comedy live-show keyword joins (Zarna voice, positive)."""

from __future__ import annotations

import secrets

# Keep under ~300 chars for single-segment SMS where possible.
_COMEDY_NEW = [
    "You're IN! Buckle up — tonight we're going to have way too much fun. "
    "Text me anytime if you have a question, or if you need Auntie Z to diplomatically "
    "explain something to your mother-in-law. See you at the show!",
    "Welcome to the chaos — in the best way! So glad you're on the list. "
    "Ping me if you need anything, or if you want backup in a friendly-but-firm conversation "
    "with the in-laws. Let's go!",
    "Officially on the list — yes! Get ready for a night of laughs. "
    "If something pops up, text me. If your MIL 'accidentally' books the same row, "
    "I'll help you handle it with grace and a little spice.",
    "You're on the roster, superstar! This is going to be electric. "
    "Questions? Text me. Need someone to cheer you on before you walk into family dinner? "
    "I'm here — positive vibes only, but we can still be clever about it.",
    "Confirmed — you're coming with us on this ride! "
    "Reach out if you have questions, or if you want me to hype you up before you deal with "
    "that one relative. Can't wait!",
    "List? You're on it. Energy? High. "
    "Message me if you need directions, pep talks, or a tactful way to say 'no' to drama. "
    "Tonight we laugh loud!",
    "Welcome aboard — comedy night just got better because you're in. "
    "Text me anytime: questions, jokes, or a quick vent about the auntie who comments on your plate. "
    "I've got you.",
    "You're set! Prepare for joy, jokes, and maybe one harmless conspiracy theory about snacks. "
    "If you need me — questions, life stuff, or backup for a tricky family text — just reply here.",
    "In! Done! Celebrating you! "
    "This show is going to slap. If you need anything before doors, text me. "
    "If you need a script for handling unsolicited advice, I can make it funny AND kind.",
    "You're officially on the guest list — let's GO! "
    "Holler if you have questions, or if you want me to roast… I mean, *lovingly roast*… "
    "a situation that's been stressing you. Positive energy, big laughs.",
]

_COMEDY_REPEAT = [
    "Already got you on the list, rockstar — you're good! "
    "Text me if you need anything before the show.",
    "I see you — still on the list, still fabulous. "
    "Questions? Just text. Otherwise save the energy for laughing!",
    "Double-tap noted — you're already in! Can't wait. Ping me if you need anything.",
]


def random_comedy_confirmation_new() -> str:
    return secrets.choice(_COMEDY_NEW)


def random_comedy_confirmation_repeat() -> str:
    return secrets.choice(_COMEDY_REPEAT)
