import os
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
from .db import init_db
from .scheduler import init_scheduler


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("OPERATOR_SECRET_KEY", os.getenv("SECRET_KEY", "change-me-in-production"))

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
