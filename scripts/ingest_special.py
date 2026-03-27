"""
Parses a comedy special transcript PDF (exported as text) and saves it
as a clean JSON file in Transcripts/specials/.

PDF format has lines like:
  00:00:09:06 - 00:00:39:00
  Unknown
  Actual spoken text here...
  -- 1 of 29 --

We strip all timestamps, "Unknown" speaker labels, and page markers,
then join the remaining lines into a single clean full_text string.
"""

import re
import json
import sys

TIMESTAMP_RE = re.compile(r"^\d{2}:\d{2}:\d{2}:\d{2}\s*-\s*\d{2}:\d{2}:\d{2}:\d{2}$")
PAGE_MARKER_RE = re.compile(r"^--\s*\d+\s*of\s*\d+\s*--$")


def clean_transcript(raw_text: str) -> str:
    lines = raw_text.splitlines()
    kept = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if TIMESTAMP_RE.match(stripped):
            continue
        if PAGE_MARKER_RE.match(stripped):
            continue
        if stripped.lower() == "unknown":
            continue
        kept.append(stripped)

    return " ".join(kept)


def ingest(input_path: str, output_path: str, special_name: str):
    with open(input_path, "r", encoding="utf-8") as f:
        raw = f.read()

    full_text = clean_transcript(raw)

    data = {
        "special": special_name,
        "full_text": full_text,
        "length": len(full_text),
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    word_count = len(full_text.split())
    print(f"Saved '{special_name}' → {output_path}")
    print(f"  {word_count:,} words, {len(full_text):,} characters")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 ingest_special.py <input_txt> <output_json> <special_name>")
        sys.exit(1)

    ingest(sys.argv[1], sys.argv[2], sys.argv[3])
