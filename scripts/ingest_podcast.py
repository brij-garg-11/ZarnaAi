#!/usr/bin/env python3
"""
Fetch all episodes from the Zarna Garg Family Podcast RSS feed and append
them as chunks to training_data/zarna_chunks.json.

Each episode chunk links to its specific YouTube video URL (matched by title).
Falls back to the channel URL if no match is found.

Safe to re-run: episodes already ingested (tracked by GUID in
training_data/podcast_guids.json) are skipped automatically.

Run whenever a new episode drops (every Monday ~noon) to keep the bot current:
    python3 scripts/ingest_podcast.py

Then rebuild embeddings:
    python3 scripts/build_embeddings.py
"""

import json
import os
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime

import requests
import scrapetube
from dotenv import load_dotenv

load_dotenv()

RSS_URL            = "https://feeds.megaphone.fm/ASTI4272864122"
YOUTUBE_CHANNEL    = "https://www.youtube.com/@ZarnaGarg"
PODCAST_LISTEN_URL = YOUTUBE_CHANNEL  # fallback if no episode match found

_BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHUNKS_PATH = os.path.join(_BASE_DIR, "training_data", "zarna_chunks.json")
GUIDS_PATH  = os.path.join(_BASE_DIR, "training_data", "podcast_guids.json")

# Patterns that mark the start of sponsor copy — everything from here down is stripped.
_SPONSOR_CUT_RE = re.compile(
    r"(?:And t|T)hank(?:s)? (?:you )?to our sponsors?"
    r"|Learn more about your ad choices"
    r"|Visit (?:www\.)?podcastchoices\.com",
    re.IGNORECASE,
)


def _clean_description(text: str) -> str:
    """Strip sponsor copy and normalise whitespace."""
    m = _SPONSOR_CUT_RE.search(text)
    if m:
        text = text[: m.start()]
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_episodes() -> list[dict]:
    """Fetch and parse the RSS feed, return a list of episode dicts."""
    resp = requests.get(RSS_URL, timeout=30)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    episodes = []
    for item in root.findall(".//item"):
        title_el = item.find("title")
        desc_el  = item.find("description")
        pub_el   = item.find("pubDate")
        guid_el  = item.find("guid")

        if title_el is None or desc_el is None:
            continue

        title       = (title_el.text or "").strip()
        description = _clean_description(desc_el.text or "")
        guid        = (guid_el.text if guid_el is not None else title).strip()

        try:
            dt       = parsedate_to_datetime((pub_el.text or "").strip())
            date_str = dt.strftime("%b %d, %Y")
        except Exception:
            date_str = (pub_el.text or "").strip()

        episodes.append(
            {"title": title, "description": description, "date": date_str, "guid": guid}
        )

    return episodes


def fetch_youtube_urls() -> dict[str, str]:
    """
    Returns a dict mapping normalised episode title → YouTube URL.

    Handles multiple YouTube title formats:
      "The Zarna Garg Family Podcast | Episode 121: What Are We Snacking On?"
      "The Zarna Garg Family Podcast | Ep. 54 Passion Vs Prestige"
      "HOLIDAY RERUN: The Zarna Garg Family Podcast | Make Money or Move Out"
    """
    print("  Fetching YouTube video list …")
    url_map: dict[str, str] = {}
    try:
        videos = scrapetube.get_channel(channel_url=YOUTUBE_CHANNEL, content_type="videos")
        for v in videos:
            yt_title = v.get("title", {}).get("runs", [{}])[0].get("text", "")
            vid_id   = v.get("videoId", "")
            if not vid_id:
                continue
            yt_url = f"https://www.youtube.com/watch?v={vid_id}"

            # Pattern 1: "... | Episode N: Title"  →  extract after last colon
            m = re.search(r":\s*(.+)$", yt_title)
            if m:
                url_map[m.group(1).strip().lower()] = yt_url

            # Pattern 2: "... | Ep. N Title"  →  extract after "Ep. N "
            m2 = re.search(r"\|\s*(?:Ep\.|Episode)\s*\d+[:\s]+(.+)$", yt_title, re.IGNORECASE)
            if m2:
                url_map[m2.group(1).strip().lower()] = yt_url

            # Pattern 3: "HOLIDAY RERUN: ... | Title"  →  extract after last pipe
            m3 = re.search(r"\|\s*(.+)$", yt_title)
            if m3:
                url_map[m3.group(1).strip().lower()] = yt_url

            # Always store full title normalised too
            url_map[yt_title.strip().lower()] = yt_url

    except Exception as e:
        print(f"  Warning: could not fetch YouTube URLs ({e}). Using channel fallback.")
    print(f"  Found {len(url_map)} YouTube title variants mapped.")
    return url_map


# RSS titles that don't exactly match any YouTube title variant — mapped manually.
_MANUAL_OVERRIDES: dict[str, str] = {
    "make money or move out (holiday rerun)":
        "https://www.youtube.com/watch?v=KzJ54X1dzPU",
    "who is the favorite child (holiday rerun)":
        "https://www.youtube.com/watch?v=Wp5y7xFfW78",
    "secrets we kept from our parents":
        "https://www.youtube.com/watch?v=rDVMK1c7rnU",
    "how to launch a second act in life - zarna garg x washington post podcasts":
        "https://www.youtube.com/watch?v=Sa7b5eGMuUk",
    "what your siblings will never tell you":
        "https://www.youtube.com/watch?v=D3P8YWTpAQo",
    "discipline in asian households":
        "https://www.youtube.com/watch?v=_ndxLjJ8mxg",
    "friendship blueprint":
        "https://www.youtube.com/watch?v=Ba0HM_3PJQ4",
}


def _normalise(title: str) -> str:
    return title.strip().lower()


def _episode_to_chunk(ep: dict, youtube_url: str) -> dict:
    text = (
        f"Podcast Episode: \"{ep['title']}\" ({ep['date']}) — "
        f"{ep['description']}\n\n"
        f"Watch/listen at: {youtube_url}"
    )
    return {"text": text, "source": "podcast_episodes"}


def _load_seen_guids() -> set:
    if os.path.exists(GUIDS_PATH):
        with open(GUIDS_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


def _save_seen_guids(guids: set) -> None:
    with open(GUIDS_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(guids), f, indent=2)


def _load_chunks() -> list:
    with open(CHUNKS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_chunks(chunks: list) -> None:
    with open(CHUNKS_PATH, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)


def main() -> None:
    print("Fetching RSS feed …")
    episodes = fetch_episodes()
    print(f"  Found {len(episodes)} episodes in feed.")

    seen_guids   = _load_seen_guids()
    new_episodes = [ep for ep in episodes if ep["guid"] not in seen_guids]
    print(f"  New episodes to ingest: {len(new_episodes)}  (already seen: {len(seen_guids)})")

    if not new_episodes:
        print("Nothing to do — all episodes already ingested.")
        return

    youtube_urls = fetch_youtube_urls()

    matched = 0
    chunks     = _load_chunks()
    new_chunks = []
    for ep in new_episodes:
        key = _normalise(ep["title"])
        yt_url = _MANUAL_OVERRIDES.get(key) or youtube_urls.get(key, PODCAST_LISTEN_URL)
        if yt_url != PODCAST_LISTEN_URL:
            matched += 1
        new_chunks.append(_episode_to_chunk(ep, yt_url))

    chunks.extend(new_chunks)
    _save_chunks(chunks)

    updated_guids = seen_guids | {ep["guid"] for ep in new_episodes}
    _save_seen_guids(updated_guids)

    print(f"  Added {len(new_chunks)} episode chunks ({matched} with specific YouTube URLs, "
          f"{len(new_chunks) - matched} using channel fallback). Total chunks: {len(chunks)}")
    print("\nNext step: rebuild embeddings with:")
    print("  python3 scripts/build_embeddings.py")


if __name__ == "__main__":
    main()
