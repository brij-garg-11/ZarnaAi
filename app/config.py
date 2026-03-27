import os

# --- Gemini ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GENERATION_MODEL = "gemini-2.5-flash"
EMBEDDING_MODEL = "gemini-embedding-001"

# --- Data ---
# Use absolute paths relative to this file so the app works regardless of
# the working directory Railway (or any other host) launches the process from.
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNKS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_chunks.json")
EMBEDDINGS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_embeddings.json.gz")

# --- Retrieval / Generation ---
TOP_K_CHUNKS = 5
CONVERSATION_HISTORY_LIMIT = 6

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
