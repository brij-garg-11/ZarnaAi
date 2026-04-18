"""
Authentication — email + password login with Flask sessions.
Passwords hashed via Werkzeug pbkdf2.

Bootstrap: set OPERATOR_BOOTSTRAP_EMAIL + OPERATOR_BOOTSTRAP_PASSWORD env vars
on first deploy and hit /operator/setup to create the owner account.
After that, manage team members from the Team page.
"""

import os
from functools import wraps

from flask import (
    Blueprint,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from ..db import get_conn

auth_bp = Blueprint("auth", __name__)


# ── Session helpers ────────────────────────────────────────────────────────

def current_user() -> dict | None:
    uid = session.get("operator_user_id")
    if not uid:
        return None
    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, email, name, is_owner, is_active, account_type, creator_slug, is_super_admin FROM operator_users WHERE id=%s",
                (uid,),
            )
            row = cur.fetchone()
        conn.close()
        if row and row["is_active"]:
            return dict(row)
    except Exception:
        pass
    return None


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            return redirect(url_for("auth.login", next=request.path))
        return f(*args, **kwargs)
    return decorated


def owner_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        u = current_user()
        if not u:
            return redirect(url_for("auth.login"))
        if not u.get("is_owner"):
            flash("Owner access required.", "error")
            return redirect(url_for("dashboard.index"))
        return f(*args, **kwargs)
    return decorated


# ── Routes ─────────────────────────────────────────────────────────────────

@auth_bp.route("/operator/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("dashboard.index"))

    error = None
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        try:
            conn = get_conn()
            import psycopg2.extras
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT * FROM operator_users WHERE email=%s AND is_active=TRUE",
                    (email,),
                )
                user = cur.fetchone()
            if user and check_password_hash(user["password_hash"], password):
                session["operator_user_id"] = user["id"]
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE operator_users SET last_login_at=NOW() WHERE id=%s",
                            (user["id"],),
                        )
                conn.close()
                return redirect(request.args.get("next") or url_for("dashboard.index"))
            conn.close()
            error = "Incorrect email or password."
        except Exception as e:
            error = "Login error — please try again."

    return render_template("login.html", error=error)


@auth_bp.route("/operator/logout")
def logout():
    session.pop("operator_user_id", None)
    return redirect(url_for("auth.login"))


@auth_bp.route("/operator/setup", methods=["GET", "POST"])
def setup():
    """
    One-time bootstrap: create the owner account.
    Only works when no operator_users exist yet, or when
    OPERATOR_BOOTSTRAP_EMAIL env var is set.
    """
    bootstrap_email = os.getenv("OPERATOR_BOOTSTRAP_EMAIL", "").strip().lower()
    bootstrap_password = os.getenv("OPERATOR_BOOTSTRAP_PASSWORD", "").strip()

    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM operator_users")
            count = cur.fetchone()[0]
        conn.close()
    except Exception:
        return render_template("setup.html", error="Database not available yet.", done=False)

    if count > 0 and not bootstrap_email:
        return render_template(
            "setup.html",
            error="Setup already complete. Use the Team page to add users.",
            done=True,
        )

    error = None
    done = False

    if request.method == "POST":
        email = (request.form.get("email") or bootstrap_email).strip().lower()
        name = (request.form.get("name") or "Owner").strip()
        password = request.form.get("password") or bootstrap_password
        confirm = request.form.get("confirm") or bootstrap_password

        if not email or not password:
            error = "Email and password are required."
        elif password != confirm:
            error = "Passwords do not match."
        elif len(password) < 8:
            error = "Password must be at least 8 characters."
        else:
            try:
                pw_hash = generate_password_hash(password)
                conn = get_conn()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """INSERT INTO operator_users (email, name, password_hash, is_owner)
                               VALUES (%s, %s, %s, TRUE)
                               ON CONFLICT (email) DO UPDATE
                               SET password_hash=%s, is_owner=TRUE, is_active=TRUE""",
                            (email, name, pw_hash, pw_hash),
                        )
                conn.close()
                done = True
            except Exception as e:
                error = f"Error creating account: {e}"

    prefill_email = bootstrap_email or ""
    return render_template("setup.html", error=error, done=done, prefill_email=prefill_email)


@auth_bp.route("/operator")
def root():
    if current_user():
        return redirect(url_for("dashboard.index"))
    return redirect(url_for("auth.login"))


@auth_bp.route("/")
def index_root():
    return redirect(url_for("auth.root"))


# ── JSON API endpoints (for React / Lovable frontend) ──────────────────────

@auth_bp.route("/api/auth/login", methods=["POST"])
def api_login():
    """
    JSON login endpoint consumed by the Zar marketing site.
    Accepts: {"email": "...", "password": "..."}
    Returns: {"success": true, "user": {...}, "redirect_to": "/operator/dashboard"}
          or {"success": false, "error": "..."}
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return jsonify(success=False, error="Email and password are required."), 400

    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT * FROM operator_users WHERE email=%s AND is_active=TRUE",
                (email,),
            )
            user = cur.fetchone()

        if user and check_password_hash(user["password_hash"], password):
            session["operator_user_id"] = user["id"]
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE operator_users SET last_login_at=NOW() WHERE id=%s",
                        (user["id"],),
                    )
            conn.close()
            return jsonify(
                success=True,
                redirect_to="/operator/dashboard",
                user={
                    "email": user["email"],
                    "name": user["name"],
                    "is_owner": user["is_owner"],
                },
            )

        conn.close()
        return jsonify(success=False, error="Incorrect email or password."), 401

    except Exception:
        return jsonify(success=False, error="Login error — please try again."), 500


@auth_bp.route("/api/auth/signup", methods=["POST"])
def api_signup():
    """
    Self-serve account creation.
    Accepts: {"email": "...", "password": "...", "name": "..."}
    Returns: {"success": true, "user": {...}, "onboarding_required": true}
          or {"success": false, "error": "..."}
    """
    data = request.get_json(silent=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    name     = (data.get("name") or "").strip()

    if not email or not password:
        return jsonify(success=False, error="Email and password are required."), 400
    if len(password) < 8:
        return jsonify(success=False, error="Password must be at least 8 characters."), 400
    if not name:
        name = email.split("@")[0].title()

    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT id FROM operator_users WHERE email=%s", (email,))
            existing = cur.fetchone()

        if existing:
            conn.close()
            return jsonify(success=False, error="An account with that email already exists."), 409

        pw_hash = generate_password_hash(password)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO operator_users
                           (email, name, password_hash, is_owner, is_active, last_login_at)
                       VALUES (%s, %s, %s, FALSE, TRUE, NOW())
                       RETURNING id""",
                    (email, name, pw_hash),
                )
                new_id = cur.fetchone()[0]

        session["operator_user_id"] = new_id
        conn.close()
        return jsonify(
            success=True,
            onboarding_required=True,
            user={"email": email, "name": name, "account_type": None, "creator_slug": None},
        )
    except Exception:
        import logging
        logging.getLogger(__name__).exception("api_signup error")
        return jsonify(success=False, error="Signup failed — please try again."), 500


@auth_bp.route("/api/auth/logout", methods=["POST"])
def api_logout():
    """JSON logout — clears session."""
    session.pop("operator_user_id", None)
    return jsonify(success=True)


# ── Google OAuth ───────────────────────────────────────────────────────────────

def _get_google_client():
    import os
    from authlib.integrations.flask_client import OAuth
    oauth = OAuth()
    oauth.init_app(current_app)
    google = oauth.register(
        name="google",
        client_id=os.getenv("GOOGLE_CLIENT_ID"),
        client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
    return google


@auth_bp.route("/api/auth/google")
def google_login():
    """
    Redirect the browser to Google's OAuth consent screen.
    Pass ?signup=true to allow brand-new self-serve signups through.
    """
    import os
    google = _get_google_client()
    redirect_uri = os.getenv(
        "GOOGLE_REDIRECT_URI",
        "https://zarnaai-production.up.railway.app/api/auth/google/callback",
    )
    # Carry signup flag through OAuth state so callback knows the intent
    signup = request.args.get("signup", "false")
    return google.authorize_redirect(redirect_uri, state=f"signup={signup}")


@auth_bp.route("/api/auth/google/callback")
def google_callback():
    """
    Handle Google's redirect back. Three scenarios:
    1. Existing user — log them in.
    2. New user with a pending invite — auto-provision from invite.
    3. New user with signup=true state — create a fresh account, land on onboarding.
    """
    import os
    import logging
    logger = logging.getLogger(__name__)
    frontend_url = os.getenv("FRONTEND_URL", "https://zar-fan-connect.lovable.app")

    try:
        google = _get_google_client()
        token = google.authorize_access_token()
        userinfo = token.get("userinfo") or google.userinfo()
        email = (userinfo.get("email") or "").strip().lower()
        name  = (userinfo.get("name") or email.split("@")[0].title()).strip()

        # Recover signup intent from state param
        state = request.args.get("state", "")
        is_signup = "signup=true" in state

        if not email:
            return redirect(f"{frontend_url}/login?error=no_email")

        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT * FROM operator_users WHERE email=%s AND is_active=TRUE",
                (email,),
            )
            user = cur.fetchone()

        if user:
            # Scenario 1: existing account — log in normally
            session["operator_user_id"] = user["id"]
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE operator_users SET last_login_at=NOW() WHERE id=%s",
                        (user["id"],),
                    )
            conn.close()
            # If they have no creator_slug yet, they still need onboarding
            if not user.get("creator_slug"):
                return redirect(f"{frontend_url}/onboarding")
            return redirect(f"{frontend_url}/dashboard")

        # No existing account — check for a pending invite first
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT * FROM operator_invites
                   WHERE email=%s AND accepted_at IS NULL
                   ORDER BY created_at DESC LIMIT 1""",
                (email,),
            )
            invite = cur.fetchone()

        if invite:
            # Scenario 2: invited user — auto-provision from invite
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO operator_users
                               (email, name, password_hash, is_owner, is_active,
                                account_type, creator_slug, last_login_at)
                           VALUES (%s, %s, '', FALSE, TRUE, %s, %s, NOW())
                           ON CONFLICT (email) DO UPDATE
                           SET is_active=TRUE, account_type=%s, creator_slug=%s,
                               name=%s, last_login_at=NOW()
                           RETURNING id""",
                        (email, name, invite["account_type"], invite["creator_slug"],
                         invite["account_type"], invite["creator_slug"], name),
                    )
                    new_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE operator_invites SET accepted_at=NOW() WHERE id=%s",
                        (invite["id"],),
                    )
            session["operator_user_id"] = new_id
            conn.close()
            return redirect(f"{frontend_url}/dashboard")

        if is_signup:
            # Scenario 3: brand-new self-serve signup via Google
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO operator_users
                               (email, name, password_hash, is_owner, is_active, last_login_at)
                           VALUES (%s, %s, '', FALSE, TRUE, NOW())
                           ON CONFLICT (email) DO UPDATE
                           SET is_active=TRUE, name=%s, last_login_at=NOW()
                           RETURNING id""",
                        (email, name, name),
                    )
                    new_id = cur.fetchone()[0]
            session["operator_user_id"] = new_id
            conn.close()
            return redirect(f"{frontend_url}/onboarding")

        # Unknown user, no invite, not a signup intent
        conn.close()
        return redirect(f"{frontend_url}/login?error=not_authorized")

    except Exception:
        logging.getLogger(__name__).exception("Google OAuth callback failed")
        return redirect(f"{frontend_url}/login?error=oauth_failed")


@auth_bp.route("/api/auth/me", methods=["GET"])
def api_me():
    """
    Returns the currently authenticated user, or 401 if not logged in.
    The React frontend can call this on load to check session state.
    """
    user = current_user()
    if not user:
        return jsonify(authenticated=False), 401
    return jsonify(
        authenticated=True,
        user={
            "email": user["email"],
            "name": user["name"],
            "is_owner": user["is_owner"],
            "account_type": user.get("account_type") or "performer",
            "creator_slug": user.get("creator_slug") or "",
            "is_super_admin": bool(user.get("is_super_admin")),
        },
    )
