"""
Fetch and save transcripts for all YouTube Shorts skits (non-podcast).
Applies known name-correction typos before saving.
After completion, generates docs/skits_review.md for user review.

Usage:
    python scripts/ingest_skits.py
"""

import json
import os
import re
import time
from pathlib import Path

import requests
import browser_cookie3
from youtube_transcript_api import YouTubeTranscriptApi as YTApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound

# ─── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
METADATA_PATH = ROOT / "Processed" / "youtube" / "video_metadata.json"
OUTPUT_DIR = ROOT / "Transcripts" / "skits"
REVIEW_DOC = ROOT / "docs" / "skits_review.md"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Name correction map (typo → correct) ─────────────────────────────────────
# Applied as whole-word, case-sensitive regex replacements.
NAME_CORRECTIONS = [
    # Zoya (daughter)
    (re.compile(r'\bsoya\b', re.IGNORECASE), "Zoya"),
    # Veer (youngest son)
    (re.compile(r'\bVir\b'), "Veer"),
    # Brij (oldest son / middle child)
    (re.compile(r'\bBridge\b'), "Brij"),
    # Shalabh (husband) — known safe variants
    (re.compile(r'\bShala\b'), "Shalabh"),
    (re.compile(r'\bShal\b'), "Shalabh"),
    (re.compile(r'\bShab\b'), "Shalabh"),
    # Dadhi = Hindi for grandma / MIL
    (re.compile(r'\bDahi\b'), "Dadhi"),
    # NOTE: "Sham" is intentionally excluded — too common an English word.
    # Flag manually in review doc instead.
]

SHAM_PATTERN = re.compile(r'\bSham\b')


def apply_corrections(text: str) -> tuple[str, list[str]]:
    """Apply name corrections, return corrected text and list of changes made."""
    changes = []
    for pattern, replacement in NAME_CORRECTIONS:
        matches = pattern.findall(text)
        if matches:
            changes.append(f"{matches[0]} → {replacement} ({len(matches)}x)")
            text = pattern.sub(replacement, text)
    # Flag "Sham" without replacing — needs human review
    sham_hits = SHAM_PATTERN.findall(text)
    if sham_hits:
        changes.append(f"⚠️  'Sham' appears {len(sham_hits)}x — review manually (may be Shalabh)")
    return text, changes


def clean_and_correct(raw_transcript) -> tuple[str, list[str]]:
    """Join raw transcript chunks into a single string, then correct typos."""
    lines = [chunk.text.strip() for chunk in raw_transcript if chunk.text.strip()]
    full_text = " ".join(lines)
    corrected, changes = apply_corrections(full_text)
    return corrected, changes


def load_skit_list():
    """Return all non-podcast Shorts from video metadata."""
    with open(METADATA_PATH, encoding="utf-8") as f:
        videos = json.load(f)
    skits = [
        v for v in videos
        if "#shorts" in v.get("title", "").lower()
        and "zarna garg family podcast" not in v.get("title", "").lower()
    ]
    return skits


def already_fetched(video_id: str) -> bool:
    return (OUTPUT_DIR / f"{video_id}_transcript.json").exists()


def _make_api() -> YTApi:
    """Build a YTApi instance authenticated with local Chrome cookies (called once)."""
    try:
        cj = browser_cookie3.chrome(domain_name=".youtube.com")
        session = requests.Session()
        session.cookies = cj
        print(f"  Loaded {sum(1 for _ in cj)} YouTube cookies from Chrome.")
        return YTApi(http_client=session)
    except Exception as e:
        print(f"  Warning: could not load Chrome cookies ({e}). Falling back to unauthenticated.")
        return YTApi()


# Module-level singleton — load cookies once at import/startup
_API: YTApi | None = None


def get_api() -> YTApi:
    global _API
    if _API is None:
        _API = _make_api()
    return _API


def fetch_transcript(video_id: str):
    try:
        return get_api().fetch(video_id)
    except (TranscriptsDisabled, NoTranscriptFound):
        return None
    except Exception as e:
        print(f"  ⚠ Unexpected error for {video_id}: {e}")
        return None


def save_transcript(video_id: str, title: str, text: str, changes: list[str]):
    path = OUTPUT_DIR / f"{video_id}_transcript.json"
    data = {
        "video_id": video_id,
        "title": title,
        "source": "youtube_short",
        "corrections_applied": changes,
        "full_text": text,
        "length": len(text),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


def generate_review_doc(results: list[dict]):
    """Write a markdown review doc: one section per skit with transcript preview."""
    lines = [
        "# Skits Transcript Review",
        "",
        f"Total skits processed: **{len(results)}**  ",
        f"With transcripts: **{sum(1 for r in results if r['has_transcript'])}**  ",
        f"No transcript available: **{sum(1 for r in results if not r['has_transcript'])}**",
        "",
        "Review each skit below. Mark corrections needed and reply so we can fix before embedding.",
        "",
        "---",
        "",
    ]

    for r in results:
        if not r["has_transcript"]:
            continue
        url = f"https://youtube.com/shorts/{r['video_id']}"
        lines.append(f"## {r['title']}")
        lines.append(f"**URL:** {url}  ")
        lines.append(f"**ID:** `{r['video_id']}`  ")
        if r["corrections"]:
            lines.append(f"**Auto-corrections:** {', '.join(r['corrections'])}  ")
        lines.append("")
        preview = r["preview"]
        lines.append(f"> {preview}")
        lines.append("")
        lines.append("---")
        lines.append("")

    if any(not r["has_transcript"] for r in results):
        lines.append("## Videos Without Available Transcripts")
        lines.append("")
        for r in results:
            if not r["has_transcript"]:
                url = f"https://youtube.com/shorts/{r['video_id']}"
                lines.append(f"- [{r['title']}]({url})")
        lines.append("")

    with open(REVIEW_DOC, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\nReview doc written → {REVIEW_DOC}")


def main():
    skits = load_skit_list()
    print(f"Skits to process: {len(skits)}")

    results = []
    fetched = 0
    skipped = 0
    failed = 0

    for i, video in enumerate(skits, start=1):
        vid = video["video_id"]
        title = video["title"]

        if already_fetched(vid):
            print(f"[{i}/{len(skits)}] SKIP (exists): {title}")
            # Load existing for review doc
            with open(OUTPUT_DIR / f"{vid}_transcript.json", encoding="utf-8") as f:
                saved = json.load(f)
            results.append({
                "video_id": vid,
                "title": title,
                "has_transcript": True,
                "corrections": saved.get("corrections_applied", []),
                "preview": saved["full_text"][:300].replace("\n", " "),
            })
            skipped += 1
            continue

        raw = fetch_transcript(vid)

        if raw is None:
            print(f"[{i}/{len(skits)}] NO TRANSCRIPT: {title}")
            results.append({
                "video_id": vid,
                "title": title,
                "has_transcript": False,
                "corrections": [],
                "preview": "",
            })
            failed += 1
        else:
            text, changes = clean_and_correct(raw)
            save_transcript(vid, title, text, changes)
            preview = text[:300].replace("\n", " ")
            print(f"[{i}/{len(skits)}] OK: {title} | {len(text)} chars | corrections: {changes}")
            results.append({
                "video_id": vid,
                "title": title,
                "has_transcript": True,
                "corrections": changes,
                "preview": preview,
            })
            fetched += 1

        # Delay to avoid triggering YouTube rate limits
        time.sleep(1.5)

    print(f"\n{'='*60}")
    print(f"Done. Fetched: {fetched} | Skipped (already saved): {skipped} | No transcript: {failed}")

    generate_review_doc(results)


if __name__ == "__main__":
    main()
