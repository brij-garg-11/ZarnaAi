#!/usr/bin/env python3
"""
Ingest Zarna-only podcast transcripts into training_data/zarna_chunks.json.

Expected input directory:
  Transcripts/podcast_zarna/

Accepted file formats:
  - .txt (plain transcript text)
  - .json with one of:
      {"full_text": "..."}
      {"segments":[{"text":"..."}, ...]}
      [{"text":"..."}, ...]

Usage:
  python3 scripts/ingest_zarna_podcast_transcripts.py

After ingesting, rebuild embeddings:
  python3 scripts/build_embeddings.py
"""

import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_DIR = BASE_DIR / "Transcripts" / "podcast_zarna"
CHUNKS_PATH = BASE_DIR / "training_data" / "zarna_chunks.json"

CHUNK_MAX_WORDS = 80
CHUNK_OVERLAP = 20


def _chunk_text(text: str, max_words: int = CHUNK_MAX_WORDS, overlap: int = CHUNK_OVERLAP) -> list[str]:
    words = text.split()
    if not words:
        return []
    step = max_words - overlap
    chunks: list[str] = []
    for i in range(0, len(words), step):
        chunk = " ".join(words[i : i + max_words]).strip()
        if len(chunk.split()) >= 10:
            chunks.append(chunk)
        if i + max_words >= len(words):
            break
    return chunks


def _extract_text_from_json(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict):
        if isinstance(data.get("full_text"), str):
            return data["full_text"].strip()
        if isinstance(data.get("segments"), list):
            return " ".join(
                str(seg.get("text", "")).strip()
                for seg in data["segments"]
                if isinstance(seg, dict) and str(seg.get("text", "")).strip()
            ).strip()
    if isinstance(data, list):
        return " ".join(
            str(seg.get("text", "")).strip()
            for seg in data
            if isinstance(seg, dict) and str(seg.get("text", "")).strip()
        ).strip()
    return ""


def _read_transcript(path: Path) -> str:
    if path.suffix.lower() == ".txt":
        return path.read_text(encoding="utf-8").strip()
    if path.suffix.lower() == ".json":
        return _extract_text_from_json(path)
    return ""


def main() -> None:
    if not INPUT_DIR.exists():
        print(f"Input directory not found: {INPUT_DIR}")
        print("Create it and add Zarna-only transcript files first.")
        return

    with CHUNKS_PATH.open("r", encoding="utf-8") as f:
        chunks = json.load(f)

    existing_texts = {c.get("text", "") for c in chunks}
    added = 0

    transcript_files = sorted(
        p for p in INPUT_DIR.iterdir() if p.suffix.lower() in {".txt", ".json"}
    )
    if not transcript_files:
        print(f"No .txt or .json files found in {INPUT_DIR}")
        return

    for path in transcript_files:
        text = _read_transcript(path)
        if not text:
            continue
        source_name = f"podcast_zarna_{path.name}"
        for chunk in _chunk_text(text):
            if chunk in existing_texts:
                continue
            chunks.append({"text": chunk, "source": source_name})
            existing_texts.add(chunk)
            added += 1

    with CHUNKS_PATH.open("w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"Added {added} Zarna-only podcast chunks.")
    print(f"Total chunks now: {len(chunks)}")
    print("Next step: python3 scripts/build_embeddings.py")


if __name__ == "__main__":
    main()
