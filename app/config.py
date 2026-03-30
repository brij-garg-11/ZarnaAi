import os

# --- Gemini ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GENERATION_MODEL = "gemini-2.5-flash"
INTENT_MODEL     = "gemini-2.5-flash"   # same model as generation — older flash versions deprecated
EMBEDDING_MODEL  = "gemini-embedding-001"

# --- Data ---
# Use absolute paths relative to this file so the app works regardless of
# the working directory Railway (or any other host) launches the process from.
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNKS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_chunks.json")
EMBEDDINGS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_embeddings.json.gz")

# --- Retrieval / Generation ---
TOP_K_CHUNKS = 7
CONVERSATION_HISTORY_LIMIT = 8

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
