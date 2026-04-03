"""Random SMS copy for comedy live-show keyword joins (Zarna voice, positive).

Structure per message: (1) you're in, (2) joke in Zarna tone, (3) enjoy the show.
Does not promise a real person will reply — this number is automated.
"""

from __future__ import annotations

import secrets

# Multi-part SMS is fine for show welcomes; keep each variant scannable.
_COMEDY_NEW = [
    (
        "You're IN — officially on the list! "
        "My MIL once said she's 'easygoing' — then reorganized my spice rack alphabetically. "
        "Enjoy the show tonight; laugh loud and leave the sorting to her. "
    ),
    (
        "You're on the list — yes! "
        "My husband can explain a 401(k) for 40 minutes but 'where are the kids' socks' is somehow my department. "
        "Enjoy the show; you've earned the night off. "
    ),
    (
        "Welcome — you're in! "
        "I told my kids we're leaving in five minutes. That was three years ago. Same energy, different venue — enjoy the show! "
    ),
    (
        "Confirmed: you're coming with us! "
        "The secret to marriage? Two people who agree the other person loads the dishwasher wrong. "
        "Enjoy the show tonight — and may your dishwasher debates wait till tomorrow. "
    ),
    (
        "You're set — roster, locked, excitement: high! "
        "My mother-in-law complimented my cooking once. I framed the text. "
        "Enjoy the show; save room for joy (and zero guilt). "
    ),
    (
        "In! Done! You're on the guest list! "
        "I asked my husband to 'watch the kids' — he watched. That's it. Just watched. "
        "Enjoy the show! "
    ),
    (
        "You're officially in — let's GO! "
        "Desi auntie at a party: 'You've gained weight!' Me: 'You've gained opinions!' "
        "Enjoy the show; tonight the only opinion that matters is laughter. "
    ),
    (
        "List? You're on it. Energy? Through the roof! "
        "My teen acted shocked I knew how Wi-Fi works. I invented patience before Bluetooth existed. "
        "Enjoy the show — you've earned every laugh. "
    ),
    (
        "Welcome aboard — comedy night is better because you're in! "
        "Kids: 'We're bored.' Also kids: reject every activity like Supreme Court justices. "
        "Enjoy the show; tonight boredom is not on the menu. "
    ),
    (
        "You're IN — buckle up for fun! "
        "Husband says 'I'll do the dishes' and enters a parallel timeline where dishes do themselves. "
        "Enjoy the show; leave the dishes to that other universe. "
    ),
    (
        "On the list — celebrating you! "
        "MIL: 'Are you sure you want seconds?' Me: 'Are you sure you want this conversation?' "
        "Enjoy the show — seconds of comedy encouraged. "
    ),
    (
        "You're coming with us — so glad! "
        "I told my kids to act natural for a photo. They looked like tiny FBI witnesses. "
        "Enjoy the show; tonight you can laugh like nobody's tagging you. "
    ),
]

_COMEDY_REPEAT = [
    (
        "Still on the list — you're good! "
        "Short version: husband 'helped' by standing near the problem. "
        "Enjoy the show! "
    ),
    (
        "Got you — already in! "
        "MIL energy: loving you loudly in public, questioning your life choices in private. "
        "Enjoy the show tonight! "
    ),
    (
        "Double yes — you're already on the roster! "
        "Kids asked what's for dinner. I said 'surprise.' They looked scared. Good. "
        "Enjoy the show! "
    ),
    (
        "Already in, still fabulous! "
        "Reminder: spice rack alphabetizing is not a love language. "
        "Enjoy the show! "
    ),
    (
        "You're still on the list — perfect! "
        "Save the family group chat drama for after the curtain. "
        "Enjoy the show! "
    ),
    (
        "Noted — you're in (twice the enthusiasm, same great seat in spirit)! "
        "Tonight we laugh; tomorrow we negotiate snack policy again. "
        "Enjoy the show! "
    ),
]


def random_comedy_confirmation_new() -> str:
    return secrets.choice(_COMEDY_NEW)


def random_comedy_confirmation_repeat() -> str:
    return secrets.choice(_COMEDY_REPEAT)


# Live stream — same shape: you're in → Zarna-tone joke → enjoy the stream (automated; no human promise).
_LIVE_STREAM_NEW = [
    (
        "You're IN — welcome to the live! "
        "My husband thinks 'buffering' is a personality trait. Tonight we're skipping him and going straight to the good part. "
        "Grab your snacks, settle in, enjoy the stream! "
    ),
    (
        "Officially on the list for the live — yes! "
        "Kids barged in during my last stream like tiny producers with zero budget. You're the audience with taste. "
        "Welcome to the live — laugh loud from wherever you are! "
    ),
    (
        "Welcome — you're in! "
        "MIL asked if I'm 'really working' when I'm on camera. I said yes — the camera doesn't lie, the Wi-Fi sometimes does. "
        "Enjoy the live stream; you've got the best seat in the house (your couch). "
    ),
    (
        "Confirmed — you're with us for the live! "
        "Desi auntie energy: 'Why are you on your phone?' Same auntie during the live: 'Send me the link.' "
        "Welcome to the live — share the energy, not the unsolicited advice! "
    ),
    (
        "You're set — live roster, locked! "
        "I told my teen to use headphones for the stream. They heard 'optional suggestion' — same as chores. "
        "Enjoy the show online; turn it up if you need to drown out background commentary! "
    ),
    (
        "List? You're on it. Live? We're doing it! "
        "Husband offered to 'handle tech.' I love him. I still clicked the link myself. "
        "Welcome to the live stream — you're officially part of the chaos! "
    ),
    (
        "You're IN — let's GO live! "
        "My MIL thinks the stream is a group call she wasn't invited to. Tonight you're invited — she can catch the replay. "
        "Enjoy the live; snacks mandatory, judgment optional! "
    ),
    (
        "Welcome aboard — the live just got better because you're here! "
        "If your Wi-Fi hiccups, blame the router — that's what I do. If the jokes land, credit the stream gods. "
        "Welcome to the live — stay for the fun! "
    ),
    (
        "You're on the guest list for the live — celebrating you! "
        "Kids asked if they can 'be in the background.' I said only if they sign a no-drama contract. "
        "Enjoy the stream from your corner of the world! "
    ),
    (
        "In! Done! You're coming to the live with us! "
        "Same energy as a show night, minus parking — you're winning. "
        "Welcome to the live — cozy clothes encouraged, good vibes required! "
    ),
]

_LIVE_STREAM_REPEAT = [
    (
        "Still on the list for the live — you're good! "
        "Double-tap noted; the stream's still the party. "
        "Welcome back to the live energy! "
    ),
    (
        "Got you — already in for the live! "
        "Like hitting refresh but emotionally. "
        "Enjoy the stream! "
    ),
    (
        "Already in — fabulous! "
        "Save the 'are you watching?' texts for after; we're live now. "
        "Welcome to the live! "
    ),
    (
        "You're still on the live list — perfect! "
        "MIL can wait; the stream cannot. "
        "Enjoy! "
    ),
    (
        "Noted twice — still in! "
        "Buffering is temporary; the hype is permanent. "
        "Welcome to the live! "
    ),
    (
        "Double yes — already rostered for the live! "
        "Your couch called — it said you're doing great. "
        "Enjoy the stream! "
    ),
]


def random_live_stream_confirmation_new() -> str:
    return secrets.choice(_LIVE_STREAM_NEW)


def random_live_stream_confirmation_repeat() -> str:
    return secrets.choice(_LIVE_STREAM_REPEAT)
