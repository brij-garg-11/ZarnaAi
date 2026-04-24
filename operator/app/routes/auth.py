"""
Authentication — email + password login with Flask sessions.
Passwords hashed via Werkzeug pbkdf2.

Bootstrap: set OPERATOR_BOOTSTRAP_EMAIL + OPERATOR_BOOTSTRAP_PASSWORD env vars
on first deploy and hit /operator/setup to create the owner account.
After that, manage team members from the Team page.
"""

import logging
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
logger = logging.getLogger(__name__)


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


def get_authorized_slugs(user_id: int, own_slug: str | None) -> set[str]:
    """
    Returns the full set of creator_slugs this user is authorized to access:
      - Their own slug (if set)
      - Any tenant_slug they have an accepted team_members row for

    This is the source of truth for all data-access authorization.
    """
    slugs: set[str] = set()
    if own_slug:
        slugs.add(own_slug)
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """SELECT tenant_slug FROM team_members
                   WHERE user_id = %s AND accepted_at IS NOT NULL""",
                (user_id,),
            )
            for row in cur.fetchall():
                slugs.add(row[0])
        conn.close()
    except Exception:
        logger.exception("get_authorized_slugs: db error for user_id=%s", user_id)
    return slugs


def resolve_slug() -> tuple[str, int | None]:
    """
    Returns (effective_slug, http_error_code_or_None).

    Rules:
      - Super-admins: can view any slug via session["viewing_as"]; no
        further membership check is applied.
      - Everyone else: effective slug = their own creator_slug OR any
        slug they have an accepted team_members record for (e.g. via
        session["viewing_as"] for account-switchers).
      - If the resolved slug is empty (account not fully set up), returns
        ("", None) — callers should treat this as empty/zero data.
      - If viewing_as is set but the user is NOT authorized for that slug,
        returns ("", 403).
    """
    user = current_user()
    if not user:
        return ("", 401)

    own_slug = user.get("creator_slug") or ""

    # Super-admins bypass the membership check.
    if user.get("is_super_admin"):
        effective = session.get("viewing_as") or own_slug
        return (effective, None)

    # For regular users, viewing_as lets team members switch accounts.
    requested = session.get("viewing_as") or own_slug
    if not requested:
        return ("", None)

    authorized = get_authorized_slugs(user["id"], own_slug)
    if requested not in authorized:
        logger.warning(
            "resolve_slug: user %s tried to access slug '%s' — not authorized (authorized: %s)",
            user.get("email"), requested, authorized,
        )
        return ("", 403)

    return (requested, None)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            # API routes (called by Lovable frontend via fetch) need 401 JSON,
            # not a 302 redirect that fetch() silently follows to an HTML page.
            if request.path.startswith("/api/"):
                from flask import jsonify as _jsonify
                return _jsonify(authenticated=False, error="Login required"), 401
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
                session.permanent = True
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
            session.permanent = True
            logger.info(
                "[AUTH] api_login success — user=%s session.permanent=%s lifetime=%s",
                user["email"],
                session.permanent,
                current_app.config.get("PERMANENT_SESSION_LIFETIME"),
            )
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
        logger.exception("[AUTH] api_login error")
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
        session.permanent = True
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


# ── Password reset ─────────────────────────────────────────────────────────────

def _send_reset_email(to_email: str, reset_url: str) -> None:
    """Send a password reset email via Resend."""
    import os
    import uuid
    import resend

    resend.api_key = os.getenv("RESEND_API_KEY", "")
    from_addr = os.getenv("RESEND_FROM", "hello@zar.bot")

    resend.Emails.send({
        "from": f"Zar <{from_addr}>",
        "to": [to_email],
        "subject": "Reset your Zar password",
        "headers": {"Message-ID": f"<reset-{uuid.uuid4().hex}@zar.bot>"},
        "html": f"""
        <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px 24px">
          <h2 style="font-size:22px;font-weight:700;margin-bottom:8px">Reset your password</h2>
          <p style="color:#555;margin-bottom:24px">
            We received a request to reset the password for your Zar account.
            Click the button below. This link expires in 1 hour.
          </p>
          <a href="{reset_url}"
             style="display:inline-block;background:#f97316;color:#fff;font-weight:600;
                    padding:12px 28px;border-radius:8px;text-decoration:none;font-size:15px">
            Reset password
          </a>
          <p style="color:#999;font-size:13px;margin-top:32px">
            If you didn't request this, you can ignore this email.
            Your password won't change until you click the link above.
          </p>
        </div>
        """,
    })


@auth_bp.route("/api/auth/forgot-password", methods=["POST"])
def api_forgot_password():
    """
    Request a password reset email.
    Always returns 200 so we don't leak whether an email is registered.
    """
    import os
    import secrets
    from datetime import datetime, timedelta, timezone

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify(success=True)

    frontend_url = os.getenv("FRONTEND_URL", "https://zar-fan-connect.lovable.app")

    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, email FROM operator_users WHERE email=%s AND is_active=TRUE",
                (email,),
            )
            user = cur.fetchone()

        if user:
            token = secrets.token_urlsafe(32)
            expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO password_reset_tokens (user_id, token, expires_at)
                           VALUES (%s, %s, %s)""",
                        (user["id"], token, expires_at),
                    )
            reset_url = f"{frontend_url}/reset-password?token={token}"
            try:
                _send_reset_email(user["email"], reset_url)
            except Exception:
                import logging
                logging.getLogger(__name__).exception(
                    "forgot_password: failed to send email to %s", email
                )
        conn.close()
    except Exception:
        import logging
        logging.getLogger(__name__).exception("forgot_password error for %s", email)

    # Always 200 — never reveal whether the email exists
    return jsonify(success=True)


@auth_bp.route("/api/auth/reset-password", methods=["POST"])
def api_reset_password():
    """
    Consume a reset token and set a new password.
    Token is single-use and expires after 1 hour.
    """
    from datetime import datetime, timezone
    from werkzeug.security import generate_password_hash

    data = request.get_json(silent=True) or {}
    token    = (data.get("token") or "").strip()
    password = (data.get("password") or "").strip()

    if not token:
        return jsonify(success=False, error="Reset token is required."), 400
    if not password or len(password) < 8:
        return jsonify(success=False, error="Password must be at least 8 characters."), 400

    try:
        conn = get_conn()
        import psycopg2.extras
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT id, user_id, expires_at, used_at
                   FROM password_reset_tokens
                   WHERE token=%s""",
                (token,),
            )
            row = cur.fetchone()

        if not row:
            return jsonify(success=False, error="Invalid or expired reset link."), 400
        if row["used_at"] is not None:
            return jsonify(success=False, error="This reset link has already been used."), 400
        if row["expires_at"] < datetime.now(timezone.utc):
            return jsonify(success=False, error="This reset link has expired. Please request a new one."), 400

        pw_hash = generate_password_hash(password)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE operator_users SET password_hash=%s WHERE id=%s",
                    (pw_hash, row["user_id"]),
                )
                cur.execute(
                    "UPDATE password_reset_tokens SET used_at=NOW() WHERE id=%s",
                    (row["id"],),
                )
        conn.close()
        return jsonify(success=True)

    except Exception:
        import logging
        logging.getLogger(__name__).exception("reset_password error")
        return jsonify(success=False, error="Something went wrong — please try again."), 500


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
        "https://api.zar.bot/api/auth/google/callback",
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
            session.permanent = True
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

        # Before invite / signup flows: check if this email belongs to an account
        # that was explicitly deactivated (e.g. removed from a team). We never
        # allow deactivated accounts to re-activate themselves — only an admin
        # re-invite can restore access.
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT id, is_active FROM operator_users WHERE email=%s",
                (email,),
            )
            existing_any = cur.fetchone()

        if existing_any and not existing_any["is_active"]:
            conn.close()
            return redirect(f"{frontend_url}/login?error=access_revoked")

        # No existing active account — check for a pending invite first
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """SELECT * FROM operator_invites
                   WHERE email=%s AND accepted_at IS NULL
                   ORDER BY created_at DESC LIMIT 1""",
                (email,),
            )
            invite = cur.fetchone()

        if invite:
            # Scenario 2: invited user — auto-provision from invite.
            # Only inserts a fresh row; never re-activates a deactivated account
            # (that case is caught above).
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO operator_users
                               (email, name, password_hash, is_owner, is_active,
                                account_type, creator_slug, last_login_at)
                           VALUES (%s, %s, '', FALSE, TRUE, %s, %s, NOW())
                           ON CONFLICT (email) DO UPDATE
                           SET account_type=%s, creator_slug=%s,
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
                    # Register the newly-accepted user in team_members so seat
                    # counting + role checks work from their first login.
                    cur.execute(
                        """INSERT INTO team_members (tenant_slug, user_id, role, invited_at, accepted_at)
                           VALUES (%s, %s, 'member', %s, NOW())
                           ON CONFLICT (tenant_slug, user_id) DO UPDATE
                           SET accepted_at = NOW()""",
                        (invite["creator_slug"], new_id, invite["created_at"]),
                    )
            session["operator_user_id"] = new_id
            session.permanent = True
            conn.close()
            return redirect(f"{frontend_url}/dashboard")

        if is_signup:
            # Scenario 3: brand-new self-serve signup via Google.
            # Only creates a genuinely new row — never re-activates a deactivated
            # account (that case is caught above).
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO operator_users
                               (email, name, password_hash, is_owner, is_active, last_login_at)
                           VALUES (%s, %s, '', FALSE, TRUE, NOW())
                           ON CONFLICT (email) DO NOTHING
                           RETURNING id""",
                        (email, name),
                    )
                    row = cur.fetchone()
                    if not row:
                        # Conflict means account exists — we shouldn't reach here
                        # because existing active accounts are handled in Scenario 1
                        # and deactivated ones are blocked above. Be safe and deny.
                        conn.close()
                        return redirect(f"{frontend_url}/login?error=not_authorized")
                    new_id = row[0]
            session["operator_user_id"] = new_id
            session.permanent = True
            conn.close()
            return redirect(f"{frontend_url}/onboarding")

        # Unknown user, no invite, not a signup intent
        conn.close()
        return redirect(f"{frontend_url}/login?error=not_authorized")

    except Exception:
        logger.exception("Google OAuth callback failed")
        return redirect(f"{frontend_url}/login?error=oauth_failed")


@auth_bp.route("/api/auth/me", methods=["GET"])
def api_me():
    """
    Returns the currently authenticated user, or 401 if not logged in.
    The React frontend can call this on load to check session state.
    """
    uid = session.get("operator_user_id")
    logger.info(
        "[AUTH] /api/auth/me — session uid=%s permanent=%s cookie=%s",
        uid,
        session.permanent,
        request.cookies.get("session", "NO_COOKIE"),
    )
    user = current_user()
    if not user:
        logger.info("[AUTH] /api/auth/me — unauthenticated (no valid user for uid=%s)", uid)
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
