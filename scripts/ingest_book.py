"""
Ingest a PDF book into the Zarna AI knowledge base.

Usage:
    cd "/Users/brijgarg/Zarna Project"
    python scripts/ingest_book.py "this american woman.pdf"

This script:
  1. Extracts text from every page of the PDF
  2. Cleans up headers, page numbers, and extra whitespace
  3. Chunks the text into ~80 word pieces with 20-word overlap
     (matches the size used for transcripts)
  4. Appends new chunks to training_data/zarna_chunks.json
     (skips duplicates so it's safe to run more than once)

After running this, re-embed everything:
    python scripts/build_embeddings.py
Then commit the updated zarna_chunks.json and zarna_embeddings.json.gz.
"""

import json
import os
import re
import sys

import pdfplumber

CHUNKS_PATH = "training_data/zarna_chunks.json"
CHUNK_MAX_WORDS = 80
CHUNK_OVERLAP = 20


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text from a PDF, page by page."""
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        print(f"PDF has {len(pdf.pages)} pages")
        for i, page in enumerate(pdf.pages, 1):
            text = page.extract_text()
            if text:
                pages.append(text)
            if i % 20 == 0:
                print(f"  Extracted {i}/{len(pdf.pages)} pages…")
    return "\n".join(pages)


def clean_text(text: str) -> str:
    """Remove page numbers, headers, and normalise whitespace."""
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        line = line.strip()
        # Skip blank lines
        if not line:
            continue
        # Skip lines that are just a page number (e.g. "42", "— 42 —")
        if re.fullmatch(r"[-–—\s\d]+", line):
            continue
        # Skip very short lines that are likely headers/footers (< 4 words)
        if len(line.split()) < 4:
            continue
        cleaned.append(line)
    # Rejoin and normalise multiple spaces
    text = " ".join(cleaned)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def chunk_text(text: str, max_words: int = CHUNK_MAX_WORDS, overlap: int = CHUNK_OVERLAP):
    words = text.split()
    chunks = []
    step = max_words - overlap
    for i in range(0, len(words), step):
        chunk = " ".join(words[i : i + max_words])
        if len(chunk.split()) >= 10:
            chunks.append(chunk)
        if i + max_words >= len(words):
            break
    return chunks


def load_existing_chunks():
    if not os.path.exists(CHUNKS_PATH):
        return []
    with open(CHUNKS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_chunks(chunks):
    with open(CHUNKS_PATH, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/ingest_book.py <path-to-pdf>")
        sys.exit(1)

    pdf_path = sys.argv[1]
    if not os.path.exists(pdf_path):
        print(f"File not found: {pdf_path}")
        sys.exit(1)

    source_name = os.path.basename(pdf_path)
    print(f"Reading: {pdf_path}")

    # Extract + clean
    raw_text = extract_text_from_pdf(pdf_path)
    clean = clean_text(raw_text)
    print(f"Extracted {len(clean.split()):,} words of clean text")

    # Chunk
    new_chunks = [
        {"text": c, "source": source_name}
        for c in chunk_text(clean)
    ]
    print(f"Created {len(new_chunks):,} new chunks")

    # Load existing and deduplicate by text
    existing = load_existing_chunks()
    existing_texts = {c["text"] for c in existing}

    to_add = [c for c in new_chunks if c["text"] not in existing_texts]
    print(f"Adding {len(to_add):,} chunks ({len(new_chunks) - len(to_add)} duplicates skipped)")

    if not to_add:
        print("Nothing new to add — already ingested?")
        return

    combined = existing + to_add
    save_chunks(combined)
    print(f"\nDone. {CHUNKS_PATH} now has {len(combined):,} total chunks.")
    print("\nNext step — re-embed everything:")
    print("    python scripts/build_embeddings.py")


if __name__ == "__main__":
    main()
