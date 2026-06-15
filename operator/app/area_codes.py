"""Area-code (NANP NPA) helpers for blast targeting.

The blast composer lets the operator target fans by area code. The audience
filter value is a free string that is either:
  - a named region preset (e.g. ``"ny"``, ``"nj"``, ``"nynj"``), or
  - a comma/space separated list of 3-digit area codes (e.g. ``"212, 718 201"``).

``parse_area_codes`` normalises either form into a deduped list of 3-digit codes
that can be matched against ``contacts.area_codes`` (a TEXT[] column).
"""
from __future__ import annotations

# Active NANP area codes by state. Kept intentionally broad (full state, not
# just the metro) so "ny"/"nj" reach every fan with a local number.
REGION_AREA_CODES: dict[str, list[str]] = {
    "ny": [
        "212", "315", "332", "347", "363", "516", "518", "585", "607", "631",
        "646", "680", "716", "718", "838", "845", "914", "917", "929", "934",
    ],
    "nj": [
        "201", "551", "609", "640", "732", "848", "856", "862", "908", "973",
    ],
}
# Convenience combo for the common "NY + NJ metro" blast.
REGION_AREA_CODES["nynj"] = REGION_AREA_CODES["ny"] + REGION_AREA_CODES["nj"]

# Friendly aliases the operator might type instead of the slug.
_REGION_ALIASES: dict[str, str] = {
    "new york": "ny",
    "newyork": "ny",
    "nyc": "ny",
    "new jersey": "nj",
    "newjersey": "nj",
    "ny/nj": "nynj",
    "nynj": "nynj",
    "ny nj": "nynj",
    "tristate": "nynj",
    "tri-state": "nynj",
}


def parse_area_codes(raw: str | None) -> list[str]:
    """Expand a blast area-code filter string into a deduped list of NPA codes.

    Accepts a region preset/alias or a list of explicit 3-digit codes.
    Unknown tokens that aren't 3 digits are ignored.
    """
    if not raw:
        return []
    text = raw.strip().lower()

    # Whole-string region match first (handles aliases with spaces/slashes).
    key = _REGION_ALIASES.get(text, text)
    if key in REGION_AREA_CODES:
        return list(dict.fromkeys(REGION_AREA_CODES[key]))

    codes: list[str] = []
    for token in text.replace(",", " ").split():
        tok = _REGION_ALIASES.get(token, token)
        if tok in REGION_AREA_CODES:
            codes.extend(REGION_AREA_CODES[tok])
        else:
            digits = "".join(c for c in token if c.isdigit())
            if len(digits) == 3:
                codes.append(digits)
    return list(dict.fromkeys(codes))
