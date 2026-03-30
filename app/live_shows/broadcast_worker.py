"""Background bulk send for live shows (does not block Flask request)."""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

BroadcastMode = str  # "loop" | "slicktext_campaign"


def start_broadcast_thread(
    job_id: int,
    show_id: int,
    body: str,
    provider_choice: str | None,
    deliver_as: str,
    broadcast_mode: BroadcastMode = "loop",
) -> None:
    threading.Thread(
        target=_run_broadcast,
        args=(job_id, show_id, body, provider_choice, deliver_as, broadcast_mode),
        daemon=True,
    ).start()


def _run_broadcast(
    job_id: int,
    show_id: int,
    body: str,
    provider_choice: str | None,
    deliver_as: str,
    broadcast_mode: BroadcastMode,
) -> None:
    from app.config import SLICKTEXT_API_KEY, SLICKTEXT_BRAND_ID
    from app.live_shows import repository as repo
    from app.messaging.broadcast import normalize_e164, resolve_broadcast_provider, run_loop_broadcast
    from app.messaging.slicktext_adapter import create_slicktext_adapter
    from app.messaging import slicktext_campaigns as st_campaigns

    try:
        phones = repo.signup_phones_for_show(show_id)
        prov = provider_choice if provider_choice in ("slicktext", "twilio") else resolve_broadcast_provider()
        wa = (deliver_as or "sms").lower() == "whatsapp"

        use_campaign = (
            broadcast_mode == "slicktext_campaign"
            and not wa
            and prov == "slicktext"
            and st_campaigns.v2_configured()
        )

        if broadcast_mode == "slicktext_campaign":
            if wa:
                repo.update_job_running(job_id, len(phones))
                repo.complete_job(
                    job_id,
                    0,
                    len(phones),
                    "SlickText campaigns are SMS only. Set broadcast channel to SMS or use Twilio loop mode.",
                )
                return
            if prov == "twilio":
                repo.update_job_running(job_id, len(phones))
                repo.complete_job(
                    job_id,
                    0,
                    len(phones),
                    "Campaign mode is SlickText-only. Choose SlickText (or Auto with v2 keys) instead of Twilio.",
                )
                return
            if not st_campaigns.v2_configured():
                repo.update_job_running(job_id, len(phones))
                repo.complete_job(
                    job_id,
                    0,
                    len(phones),
                    "SlickText campaign mode needs v2 API: SLICKTEXT_API_KEY + SLICKTEXT_BRAND_ID. "
                    "Legacy v1 accounts should use “One-by-one” mode.",
                )
                return

        if use_campaign:
            repo.update_job_running(job_id, len(phones))

            def progress(done: int, synced: int, failed_cnt: int) -> None:
                repo.update_job_progress(job_id, synced, failed_cnt)

            list_name = f"Zarna Live Show {show_id} Job {job_id}"
            camp_name = f"Zarna show {show_id} (job {job_id})"
            result = st_campaigns.run_live_show_campaign(
                api_key=SLICKTEXT_API_KEY,
                brand_id=str(SLICKTEXT_BRAND_ID),
                list_name=list_name,
                campaign_name=camp_name,
                body_text=body,
                phones_e164=[normalize_e164(p) for p in phones],
                progress=progress,
            )
            if result.ok:
                note = f"SlickText campaign_id={result.campaign_id} (queued/sent per SlickText)."
                if result.error:
                    note += " " + result.error
                repo.complete_job(
                    job_id,
                    result.contacts_synced,
                    result.contacts_failed,
                    note.strip() or None,
                )
            else:
                repo.complete_job(
                    job_id,
                    result.contacts_synced,
                    result.contacts_failed,
                    result.error,
                )
            return

        # --- per-number loop (SlickText v1/v2 or Twilio) ---
        repo.update_job_running(job_id, len(phones))
        slick = create_slicktext_adapter()

        def prog(_done: int, succeeded: int, failed: int) -> None:
            repo.update_job_progress(job_id, succeeded, failed)

        res = run_loop_broadcast(
            phones=phones,
            body=body,
            provider=prov,
            deliver_whatsapp=wa,
            slicktext_send=slick.send_reply,
            progress=prog,
        )
        err = "; ".join(res.errors) if res.errors else None
        repo.complete_job(job_id, res.succeeded, res.failed, err)
    except Exception as e:
        logger.exception("Live broadcast job %s failed", job_id)
        try:
            repo.complete_job(job_id, 0, 0, str(e))
        except Exception:
            pass
