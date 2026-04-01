import os
from flask import Flask
from .db import init_db
from .scheduler import init_scheduler


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = os.getenv("OPERATOR_SECRET_KEY", os.getenv("SECRET_KEY", "change-me-in-production"))

    # Register blueprints
    from .routes.auth import auth_bp
    from .routes.dashboard import dashboard_bp
    from .routes.shows import shows_bp
    from .routes.blast import blast_bp
    from .routes.team import team_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(shows_bp)
    app.register_blueprint(blast_bp)
    app.register_blueprint(team_bp)

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
