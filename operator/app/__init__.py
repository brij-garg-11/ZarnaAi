import re
import os
from datetime import timedelta
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from .db import init_db
from .scheduler import init_scheduler

# Origins allowed to call the JSON API endpoints.
# In production this will be the Lovable / Zar marketing site domain.
# In development, Lovable's preview URLs and localhost are also allowed.
_CORS_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:5173,http://localhost:8080,https://zar.bot,https://www.zar.bot,https://api.zar.bot,https://zar.com,https://www.zar.com,https://zar-fan-connect.lovable.app,https://lovable.dev,https://gptengineer.app",
    ).split(",")
    if o.strip()
]


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("OPERATOR_SECRET_KEY", os.getenv("SECRET_KEY", "change-me-in-production"))
    app.config["SESSION_PERMANENT"] = True
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=int(os.getenv("SESSION_LIFETIME_DAYS", "30")))
    app.config["SESSION_REFRESH_EACH_REQUEST"] = True

    # Allow cross-origin requests to /api/* from the marketing site.
    # supports_credentials=True is required for the session cookie to be sent.
    # flask-cors treats strings containing regex special chars as regex patterns,
    # so r"https://.*\.lovable\.app" matches all Lovable preview subdomains.
    CORS(
        app,
        resources={r"/api/*": {"origins": _CORS_ORIGINS + [
            r"https://.*\.lovable\.app",
            r"https://lovable\.dev",
            r"https://.*\.gptengineer\.app",
        ]}},
        supports_credentials=True,
    )

    app.config["SESSION_COOKIE_SECURE"] = True

    # When SESSION_COOKIE_DOMAIN is set (e.g. .zar.bot), the frontend and backend
    # share the same eTLD+1 so the cookie is first-party — SameSite=Lax works and
    # Chrome won't block it. Without a shared domain (local dev / raw Railway URL)
    # we fall back to SameSite=None so cross-origin credentialed requests still work.
    cookie_domain = os.getenv("SESSION_COOKIE_DOMAIN")
    if cookie_domain:
        app.config["SESSION_COOKIE_DOMAIN"] = cookie_domain
        app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    else:
        app.config["SESSION_COOKIE_SAMESITE"] = "None"

    # Railway terminates TLS at its proxy and forwards requests as plain HTTP.
    # ProxyFix makes request.scheme, request.host, and url_for() reflect the
    # real HTTPS origin so uploaded image URLs come out as https://, which
    # SlickText/Twilio can fetch without being redirected.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

    # Register blueprints
    from .routes.auth import auth_bp
    from .routes.dashboard import dashboard_bp
    from .routes.shows import shows_bp
    from .routes.blast import blast_bp
    from .routes.team import team_bp
    from .routes.smb_portal import smb_portal_bp
    from .routes.api import api_bp
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(shows_bp)
    app.register_blueprint(blast_bp)
    app.register_blueprint(team_bp)
    app.register_blueprint(smb_portal_bp)
    app.register_blueprint(api_bp)

    # ── CSRF protection for state-changing API requests ────────────────────
    # All /api/* POST/PUT/PATCH/DELETE requests must originate from an allowed
    # origin. Browsers always send Origin on cross-origin requests; same-origin
    # requests from the Railway host itself are also permitted.
    # This rejects any cross-site request forgery attempt where a malicious
    # page tries to trigger a credentialed POST to our API.
    _CSRF_EXEMPT_PATHS = {"/api/auth/google/callback"}
    _CSRF_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
    _ALLOWED_ORIGIN_PATTERNS = [
        re.compile(r"^https?://localhost(:\d+)?$"),
        re.compile(r"^https://.*\.lovable\.app$"),
        re.compile(r"^https://lovable\.dev$"),
        re.compile(r"^https://.*\.gptengineer\.app$"),
    ] + [re.compile(r"^" + re.escape(o) + r"$") for o in _CORS_ORIGINS]

    def _origin_allowed(origin: str) -> bool:
        return any(p.match(origin) for p in _ALLOWED_ORIGIN_PATTERNS)

    @app.before_request
    def enforce_csrf():
        if request.method not in _CSRF_METHODS:
            return
        if not request.path.startswith("/api/"):
            return
        if request.path in _CSRF_EXEMPT_PATHS:
            return
        origin = request.headers.get("Origin") or request.headers.get("Referer", "")
        # Strip path from Referer to get origin
        if origin and not origin.startswith("http"):
            return  # malformed, block
        if origin:
            # Normalise Referer to scheme+host
            from urllib.parse import urlparse
            parsed = urlparse(origin)
            check = f"{parsed.scheme}://{parsed.netloc}"
            if not _origin_allowed(check):
                return jsonify(error="CSRF check failed"), 403

    # Health check
    @app.route("/health")
    def health():
        return {"status": "ok"}

    # Run DB migrations on startup
    with app.app_context():
        init_db()

    # Start background scheduler for scheduled blasts
    init_scheduler(app)

    return app
