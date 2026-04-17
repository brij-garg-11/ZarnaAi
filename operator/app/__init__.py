import os
from flask import Flask
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
        "http://localhost:3000,http://localhost:5173,https://zar.com,https://www.zar.com,https://zar-fan-connect.lovable.app,https://zarnaai-production.up.railway.app",
    ).split(",")
    if o.strip()
]


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("OPERATOR_SECRET_KEY", os.getenv("SECRET_KEY", "change-me-in-production"))

    # Allow cross-origin requests to /api/* from the marketing site.
    # supports_credentials=True is required for the session cookie to be sent.
    CORS(
        app,
        resources={r"/api/*": {"origins": _CORS_ORIGINS}},
        supports_credentials=True,
    )

    # In production the session cookie must be shared across zar.com subdomains.
    # Set SESSION_COOKIE_DOMAIN=.zar.com in Railway env vars when going live.
    cookie_domain = os.getenv("SESSION_COOKIE_DOMAIN")
    if cookie_domain:
        app.config["SESSION_COOKIE_DOMAIN"] = cookie_domain
        app.config["SESSION_COOKIE_SAMESITE"] = "None"
        app.config["SESSION_COOKIE_SECURE"] = True

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
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(shows_bp)
    app.register_blueprint(blast_bp)
    app.register_blueprint(team_bp)
    app.register_blueprint(smb_portal_bp)

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
