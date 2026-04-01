from flask import Blueprint, render_template
from ..routes.auth import login_required, current_user
from ..queries import get_overview_stats

dashboard_bp = Blueprint("dashboard", __name__)


@dashboard_bp.route("/operator/dashboard")
@login_required
def index():
    try:
        stats = get_overview_stats()
    except Exception:
        stats = {}

    user = current_user()
    week_delta = _pct_delta(stats.get("messages_week", 0), stats.get("messages_prev_week", 0))
    sub_delta = _pct_delta(stats.get("new_subs_week", 0), stats.get("new_subs_prev_week", 0))

    return render_template(
        "dashboard.html",
        user=user,
        stats=stats,
        week_delta=week_delta,
        sub_delta=sub_delta,
    )


def _pct_delta(current: int, previous: int) -> dict:
    if previous == 0:
        return {"pct": None, "dir": "neutral"}
    diff = current - previous
    pct = round(abs(diff) / previous * 100)
    return {"pct": pct, "dir": "up" if diff > 0 else "down"}
