import os

# --- Gemini ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GENERATION_MODEL = os.getenv("GENERATION_MODEL", "gemini-2.5-flash")
INTENT_MODEL     = os.getenv("INTENT_MODEL", "gemini-2.5-flash")  # older flash versions deprecated
ROUTER_MODEL     = os.getenv("ROUTER_MODEL", "gemini-2.5-flash")  # complexity routing (fast/cheap)
EMBEDDING_MODEL  = os.getenv("EMBEDDING_MODEL", "gemini-embedding-001")

# --- Multi-model replies (optional; falls back to Gemini if keys missing) ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
# Mid tier (conversation): override to your org’s mini model ID, e.g. gpt-4o-mini
MID_MODEL = os.getenv("MID_MODEL", "gpt-4o-mini")
# High tier (nuanced / long / advice): override to current Claude Sonnet ID
HIGH_MODEL = os.getenv("HIGH_MODEL", "claude-sonnet-4-20250514")
# auto | on | off — off forces all replies through Gemini even if keys are set
MULTI_MODEL_REPLY = os.getenv("MULTI_MODEL_REPLY", "auto").strip().lower()

# --- Data ---
# Use absolute paths relative to this file so the app works regardless of
# the working directory Railway (or any other host) launches the process from.
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNKS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_chunks.json")
EMBEDDINGS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_embeddings.json.gz")

# --- Retrieval / Generation ---
TOP_K_CHUNKS = int(os.getenv("TOP_K_CHUNKS", "7"))
CONVERSATION_HISTORY_LIMIT = 8

# --- Routing fast path (skip Flash router API when safe) ---
ROUTER_SKIP_MAX_CHARS = int(os.getenv("ROUTER_SKIP_MAX_CHARS", "88"))
ROUTER_SKIP_MAX_WORDS = int(os.getenv("ROUTER_SKIP_MAX_WORDS", "12"))

# --- Ops logging (one line per reply when on) ---
LOG_REPLY_METRICS = os.getenv("LOG_REPLY_METRICS", "on").strip().lower() not in (
    "0",
    "false",
    "off",
)

# --- SlickText ---
# The adapter auto-detects which API version to use based on which keys are present:
#
# Legacy accounts (dashboard URL = www.slicktext.com/dashboard/) → v1
#   SLICKTEXT_PUBLIC_KEY   Dashboard → My Account → API → Public Key
#   SLICKTEXT_PRIVATE_KEY  Dashboard → My Account → API → Private Key
#   SLICKTEXT_TEXTWORD_ID  Dashboard → Textwords → (your keyword) → ID in the URL
#
# New accounts (created after Jan 22, 2025, dashboard URL = app.slicktext.com) → v2
#   SLICKTEXT_API_KEY      Dashboard → Settings → API & Webhooks → API Keys
#   SLICKTEXT_BRAND_ID     Same page, shown alongside the key

# v1 (legacy)
SLICKTEXT_PUBLIC_KEY  = os.getenv("SLICKTEXT_PUBLIC_KEY", "")
SLICKTEXT_PRIVATE_KEY = os.getenv("SLICKTEXT_PRIVATE_KEY", "")
SLICKTEXT_TEXTWORD_ID = os.getenv("SLICKTEXT_TEXTWORD_ID", "")

# v2 (new accounts)
SLICKTEXT_API_KEY  = os.getenv("SLICKTEXT_API_KEY", "")
SLICKTEXT_BRAND_ID = os.getenv("SLICKTEXT_BRAND_ID", "")

# --- Inbound webhook hardening (see app/inbound_security.py + main.py) ---
# SLICKTEXT_WEBHOOK_SECRET — if set, POST /slicktext/webhook must include header:
#   X-Zarna-Webhook-Secret: <same value>   (use a long random string; store only in Railway/env)
# API_SECRET_KEY — required in production for POST /message (X-Api-Key header).
# LOG_SENSITIVE_WEBHOOK_DATA — if true, log full SlickText payloads and Twilio bodies (default off).
# AI_REPLY_MAX_CONCURRENT — max simultaneous AI reply jobs per worker (default 16); extra inbound gets 503.

# --- Twilio ---
# console.twilio.com → Account Dashboard → Account SID + Auth Token
# Phone number in E.164 format, e.g. +18557689537
TWILIO_ACCOUNT_SID  = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")

# --- Live show broadcasts (optional) ---
# LIVE_SHOW_BROADCAST_PROVIDER = slicktext | twilio | auto (default: auto)
# LIVE_SHOW_BROADCAST_DELAY_MS — ms between each outbound API call in loop mode (default 350)
# TWILIO_MESSAGING_SERVICE_SID — if set, Twilio bulk uses MessagingServiceSid instead of From number
# SLICKTEXT_CAMPAIGN_DELETE_TEMP_LIST — true to DELETE the temp list after queuing a campaign (default false;
#   deleting too early may break sends; only enable if SlickText confirms it is safe for your account)
