"""Area-code (NANP NPA) helpers for blast targeting.

The blast composer lets the operator target fans by US state. The audience
filter value is a free string that is either:
  - one or more state codes / names (e.g. ``"ga"``, ``"georgia"``, ``"ca,la"``),
  - a named multi-state combo (e.g. ``"nynj"`` / ``"tristate"``), or
  - a comma/space separated list of explicit 3-digit area codes
    (e.g. ``"212, 718 201"``).

``parse_area_codes`` normalises any of these into a deduped list of 3-digit
codes that can be matched against the area code derived from each contact's
phone number (see queries._PHONE_NPA_SQL) or the ``contacts.area_codes`` column.
"""
from __future__ import annotations

# Geographic NANP area codes by US state (+ DC). Kept broad (the whole state,
# including overlays) so a state preset reaches every fan with a local number.
STATE_AREA_CODES: dict[str, list[str]] = {
    "al": ["205", "251", "256", "334", "659", "938"],
    "ak": ["907"],
    "az": ["480", "520", "602", "623", "928"],
    "ar": ["479", "501", "870"],
    "ca": ["209", "213", "279", "310", "323", "341", "350", "408", "415", "424",
           "442", "510", "530", "559", "562", "619", "626", "628", "650", "657",
           "661", "669", "707", "714", "747", "760", "805", "818", "820", "831",
           "840", "858", "909", "916", "925", "949", "951"],
    "co": ["303", "719", "720", "970", "983"],
    "ct": ["203", "475", "860", "959"],
    "de": ["302"],
    "dc": ["202"],
    "fl": ["239", "305", "321", "352", "386", "407", "448", "561", "656", "689",
           "727", "754", "772", "786", "813", "850", "863", "904", "941", "954"],
    "ga": ["229", "404", "470", "478", "678", "706", "762", "770", "912", "943"],
    "hi": ["808"],
    "id": ["208", "986"],
    "il": ["217", "224", "309", "312", "331", "447", "464", "618", "630", "708",
           "730", "773", "779", "815", "847", "872"],
    "in": ["219", "260", "317", "463", "574", "765", "812", "930"],
    "ia": ["319", "515", "563", "641", "712"],
    "ks": ["316", "620", "785", "913"],
    "ky": ["270", "364", "502", "606", "859"],
    "la": ["225", "318", "337", "504", "985"],
    "me": ["207"],
    "md": ["240", "301", "410", "443", "667"],
    "ma": ["339", "351", "413", "508", "617", "774", "781", "857", "978"],
    "mi": ["231", "248", "269", "313", "517", "586", "616", "679", "734", "810",
           "906", "947", "989"],
    "mn": ["218", "320", "507", "612", "651", "763", "952"],
    "ms": ["228", "601", "662", "769"],
    "mo": ["314", "417", "557", "573", "636", "660", "816", "975"],
    "mt": ["406"],
    "ne": ["308", "402", "531"],
    "nv": ["702", "725", "775"],
    "nh": ["603"],
    "nj": ["201", "551", "609", "640", "732", "848", "856", "862", "908", "973"],
    "nm": ["505", "575"],
    "ny": ["212", "315", "332", "347", "363", "516", "518", "585", "607", "631",
           "646", "680", "716", "718", "838", "845", "914", "917", "929", "934"],
    "nc": ["252", "336", "704", "743", "828", "910", "919", "980", "984"],
    "nd": ["701"],
    "oh": ["216", "220", "234", "326", "330", "380", "419", "440", "513", "567",
           "614", "740", "937"],
    "ok": ["405", "539", "580", "918"],
    "or": ["458", "503", "541", "971"],
    "pa": ["215", "223", "267", "272", "412", "445", "484", "570", "582", "610",
           "717", "724", "814", "835", "878"],
    "ri": ["401"],
    "sc": ["803", "839", "843", "854", "864"],
    "sd": ["605"],
    "tn": ["423", "615", "629", "731", "865", "901", "931"],
    "tx": ["210", "214", "254", "281", "325", "346", "361", "409", "430", "432",
           "469", "512", "682", "713", "726", "737", "806", "817", "830", "832",
           "903", "915", "936", "940", "945", "956", "972", "979"],
    "ut": ["385", "435", "801"],
    "vt": ["802"],
    "va": ["276", "434", "540", "571", "703", "757", "804", "826", "948"],
    "wa": ["206", "253", "360", "425", "509", "564"],
    "wv": ["304", "681"],
    "wi": ["262", "274", "414", "534", "608", "715", "920"],
    "wy": ["307"],
}

# Backwards-compatible alias kept for the original NY+NJ tri-state blast use case.
REGION_AREA_CODES: dict[str, list[str]] = dict(STATE_AREA_CODES)
REGION_AREA_CODES["nynj"] = STATE_AREA_CODES["ny"] + STATE_AREA_CODES["nj"]

# Full state names (and a few common variants) → state code.
STATE_NAME_TO_CODE: dict[str, str] = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct", "delaware": "de",
    "district of columbia": "dc", "washington dc": "dc", "washington d.c.": "dc",
    "florida": "fl", "georgia": "ga", "hawaii": "hi", "idaho": "id",
    "illinois": "il", "indiana": "in", "iowa": "ia", "kansas": "ks",
    "kentucky": "ky", "louisiana": "la", "maine": "me", "maryland": "md",
    "massachusetts": "ma", "michigan": "mi", "minnesota": "mn", "mississippi": "ms",
    "missouri": "mo", "montana": "mt", "nebraska": "ne", "nevada": "nv",
    "new hampshire": "nh", "new jersey": "nj", "new mexico": "nm", "new york": "ny",
    "north carolina": "nc", "north dakota": "nd", "ohio": "oh", "oklahoma": "ok",
    "oregon": "or", "pennsylvania": "pa", "rhode island": "ri",
    "south carolina": "sc", "south dakota": "sd", "tennessee": "tn", "texas": "tx",
    "utah": "ut", "vermont": "vt", "virginia": "va", "washington": "wa",
    "west virginia": "wv", "wisconsin": "wi", "wyoming": "wy",
}

# Multi-state combos / friendly aliases the operator might type.
_REGION_ALIASES: dict[str, str] = {
    "nyc": "ny",
    "ny/nj": "nynj", "nynj": "nynj", "ny nj": "nynj",
    "tristate": "nynj", "tri-state": "nynj",
    **STATE_NAME_TO_CODE,
}


def _expand_token(token: str) -> list[str]:
    """Expand a single token into a list of NPA codes (empty if unknown)."""
    tok = _REGION_ALIASES.get(token, token)
    if tok in REGION_AREA_CODES:
        return list(REGION_AREA_CODES[tok])
    digits = "".join(c for c in token if c.isdigit())
    return [digits] if len(digits) == 3 else []


def parse_area_codes(raw: str | None) -> list[str]:
    """Expand a blast area-code filter string into a deduped list of NPA codes.

    Accepts state codes/names, multi-state combos/aliases, and explicit 3-digit
    codes — in any comma/space separated mix. Unknown tokens are ignored.
    """
    if not raw:
        return []
    text = raw.strip().lower()

    # Whole-string match first (handles aliases with spaces/slashes, e.g.
    # "new york", "ny/nj").
    key = _REGION_ALIASES.get(text, text)
    if key in REGION_AREA_CODES:
        return list(dict.fromkeys(REGION_AREA_CODES[key]))

    codes: list[str] = []
    for token in text.replace(",", " ").split():
        codes.extend(_expand_token(token))
    return list(dict.fromkeys(codes))
