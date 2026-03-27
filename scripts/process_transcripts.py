import os
import json

TRANSCRIPT_DIRS = [
    "Transcripts/youtube",
    "Transcripts/specials",
]
OUTPUT_PATH = "training_data/zarna_chunks.json"

def chunk_text(text, max_words=80, overlap=20):
    words = text.split()
    chunks = []

    step = max_words - overlap

    for i in range(0, len(words), step):
        chunk = " ".join(words[i:i + max_words])

        if chunk.strip():
            chunks.append(chunk)

        if i + max_words >= len(words):
            break

    return chunks


def process_all_transcripts():
    all_chunks = []

    for transcript_dir in TRANSCRIPT_DIRS:
        if not os.path.exists(transcript_dir):
            continue

        for filename in os.listdir(transcript_dir):
            if not filename.endswith(".json"):
                continue

            path = os.path.join(transcript_dir, filename)

            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                text = data.get("full_text", "")
            elif isinstance(data, list):
                lines = []
                for chunk in data:
                    chunk_value = chunk.get("text", "").strip()
                    if chunk_value:
                        lines.append(chunk_value)
                text = " ".join(lines)
            else:
                text = ""

            if not text:
                continue

            chunks = chunk_text(text)

            for chunk in chunks:
                if len(chunk.split()) < 10:
                    continue

                all_chunks.append({
                    "text": chunk,
                    "source": filename,
                })

    return all_chunks


def save_chunks(chunks):
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(chunks)} chunks")


if __name__ == "__main__":
    chunks = process_all_transcripts()
    save_chunks(chunks)