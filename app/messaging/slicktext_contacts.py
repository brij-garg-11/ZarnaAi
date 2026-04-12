from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable

import requests


DEFAULT_TEXTWORDS: tuple[tuple[int, str], ...] = (
    (3185378, "zarna"),
    (4633842, "hello"),
)
DEFAULT_PAGE_SIZE = 200
DEFAULT_BACKFILL_THRESHOLD = "2026-03-26"


@dataclass(frozen=True)
class SyncStats:
    fetched: int = 0
    unique: int = 0
    with_dates: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    total_contacts_after: int = 0


def parse_textword_config(raw: str | None) -> list[tuple[int, str]]:
    """
    Parse `id[:label],id[:label]` into [(id, label)].
    Falls back to the legacy zarna/hello pair if the env var is unset.
    """
    if not raw or not raw.strip():
        return list(DEFAULT_TEXTWORDS)

    parsed: list[tuple[int, str]] = []
    for chunk in raw.split(","):
        item = chunk.strip()
        if not item:
            continue
        textword_raw, sep, label_raw = item.partition(":")
        textword_id = int(textword_raw.strip())
        label = (label_raw.strip() if sep else "") or f"textword-{textword_id}"
        parsed.append((textword_id, label))

    if not parsed:
        raise ValueError("No SlickText textwords configured.")
    return parsed


def parse_slicktext_subscribed_date(raw: str | None) -> str | None:
    """Return the original timestamp string when it matches SlickText's format."""
    if not raw:
        return None
    try:
        datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return raw.strip()


def classify_sync_action(
    existing_created_at: datetime | None,
    subscribed_date: str | None,
    backfill_threshold: str = DEFAULT_BACKFILL_THRESHOLD,
) -> str:
    """
    Decide whether a contact would be inserted, updated, or skipped.
    """
    if existing_created_at is None:
        return "insert"
    if not subscribed_date:
        return "skip"

    threshold = date.fromisoformat(backfill_threshold)
    existing_text = existing_created_at.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    if existing_created_at.date() >= threshold and existing_text != subscribed_date:
        return "update"
    return "skip"


def fetch_contacts_for_textword(
    *,
    public_key: str,
    private_key: str,
    textword_id: int,
    label: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    client=requests,
) -> tuple[list[tuple[str, str | None]], int]:
    contacts: list[tuple[str, str | None]] = []
    offset = 0
    total = 0

    while True:
        resp = client.get(
            "https://api.slicktext.com/v1/contacts/",
            params={"textword": textword_id, "limit": page_size, "offset": offset},
            auth=(public_key, private_key),
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"SlickText API error for '{label}' ({textword_id}): {resp.status_code} {resp.text[:200]}")

        data = resp.json()
        if not total:
            total = int(data.get("meta", {}).get("total") or 0)
        page_contacts = data.get("contacts", [])
        if not page_contacts:
            break

        for contact in page_contacts:
            number = (contact.get("number") or "").strip()
            if not number:
                continue
            contacts.append((number, parse_slicktext_subscribed_date(contact.get("subscribedDate"))))

        offset += page_size
        if offset >= total:
            break

    return contacts, total


def fetch_unique_contacts(
    *,
    public_key: str,
    private_key: str,
    textwords: Iterable[tuple[int, str]],
    page_size: int = DEFAULT_PAGE_SIZE,
    client=requests,
) -> tuple[list[tuple[str, str | None]], SyncStats]:
    all_contacts: list[tuple[str, str | None]] = []
    seen: dict[str, str | None] = {}
    fetched = 0

    for textword_id, label in textwords:
        contacts, _ = fetch_contacts_for_textword(
            public_key=public_key,
            private_key=private_key,
            textword_id=textword_id,
            label=label,
            page_size=page_size,
            client=client,
        )
        fetched += len(contacts)
        for number, subscribed_date in contacts:
            if number not in seen:
                seen[number] = subscribed_date

    for number, subscribed_date in seen.items():
        all_contacts.append((number, subscribed_date))

    return all_contacts, SyncStats(
        fetched=fetched,
        unique=len(all_contacts),
        with_dates=sum(1 for _, subscribed_date in all_contacts if subscribed_date),
    )


def _dsn(database_url: str) -> str:
    return database_url.replace("postgres://", "postgresql://", 1)


def sync_contacts_to_postgres(
    *,
    database_url: str,
    contacts: Iterable[tuple[str, str | None]],
    backfill_threshold: str = DEFAULT_BACKFILL_THRESHOLD,
    dry_run: bool = False,
) -> SyncStats:
    import psycopg2
    import psycopg2.extras

    contact_list = list(contacts) if not isinstance(contacts, list) else contacts
    conn = psycopg2.connect(_dsn(database_url))

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TEMP TABLE tmp_slicktext_contacts (
                        phone_number TEXT PRIMARY KEY,
                        subscribed_date TEXT
                    ) ON COMMIT DROP
                    """
                )
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO tmp_slicktext_contacts (phone_number, subscribed_date) VALUES %s",
                    contact_list,
                    template="(%s, %s)",
                    page_size=1000,
                )

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM tmp_slicktext_contacts t
                    LEFT JOIN contacts c ON c.phone_number = t.phone_number
                    WHERE c.phone_number IS NULL
                    """
                )
                inserted = int(cur.fetchone()[0] or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM tmp_slicktext_contacts t
                    JOIN contacts c ON c.phone_number = t.phone_number
                    WHERE t.subscribed_date IS NOT NULL
                      AND c.created_at::date >= %s::date
                      AND to_char(c.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') <> t.subscribed_date
                    """,
                    (backfill_threshold,),
                )
                updated = int(cur.fetchone()[0] or 0)
                skipped = max(0, len(contact_list) - inserted - updated)

                if not dry_run:
                    cur.execute(
                        """
                        INSERT INTO contacts (phone_number, source, created_at)
                        SELECT phone_number, 'slicktext', subscribed_date::timestamp
                        FROM tmp_slicktext_contacts
                        WHERE subscribed_date IS NOT NULL
                        ON CONFLICT (phone_number) DO UPDATE
                          SET created_at = EXCLUDED.created_at
                        WHERE contacts.created_at::date >= %s::date
                          AND to_char(contacts.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                              <> to_char(EXCLUDED.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
                        """,
                        (backfill_threshold,),
                    )
                    cur.execute(
                        """
                        INSERT INTO contacts (phone_number, source)
                        SELECT phone_number, 'slicktext'
                        FROM tmp_slicktext_contacts
                        WHERE subscribed_date IS NULL
                        ON CONFLICT (phone_number) DO NOTHING
                        """
                    )

                if dry_run:
                    conn.rollback()

                cur.execute("SELECT COUNT(DISTINCT phone_number) FROM contacts")
                total_contacts_after = int(cur.fetchone()[0] or 0)
    finally:
        conn.close()

    return SyncStats(
        fetched=len(contact_list),
        unique=len(contact_list),
        with_dates=sum(1 for _, subscribed_date in contact_list if subscribed_date),
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        total_contacts_after=total_contacts_after,
    )
