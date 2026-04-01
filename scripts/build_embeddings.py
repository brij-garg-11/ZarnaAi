import gzip
import json
import os
import time
from dotenv import load_dotenv
load_dotenv()
from google import genai
from google.genai import errors as genai_errors

INPUT_PATH  = "training_data/zarna_chunks.json"
OUTPUT_PATH = "training_data/zarna_embeddings.json.gz"  # compressed — matches what the app reads

API_KEY = os.getenv("GEMINI_API_KEY", "")
EMBEDDING_MODEL = "gemini-embedding-001"
BATCH_SIZE = 50

client = genai.Client(api_key=API_KEY)


def embed_batch_with_retry(texts, max_retries=6):
    """Embed a batch of texts, retrying on 429 with exponential backoff."""
    delay = 15
    for attempt in range(max_retries):
        try:
            return client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=texts,
            )
        except genai_errors.ClientError as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                if attempt < max_retries - 1:
                    print(f"  Rate limited — waiting {delay}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(delay)
                    delay = min(delay * 2, 120)
                else:
                    raise
            else:
                raise


def embed_chunks(chunks):
    embedded = []
    total = len(chunks)

    for i in range(0, total, BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        texts = [c["text"] for c in batch]

        result = embed_batch_with_retry(texts)

        for j, emb in enumerate(result.embeddings):
            embedded.append({
                "text":      batch[j]["text"],
                "source":    batch[j]["source"],
                "embedding": emb.values,
            })

        done = min(i + BATCH_SIZE, total)
        print(f"Embedded {done}/{total} chunks")

        if done < total:
            time.sleep(0.5)

    return embedded


if __name__ == "__main__":
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    print(f"Loaded {len(chunks)} chunks")

    embedded = embed_chunks(chunks)

    with gzip.open(OUTPUT_PATH, "wt", encoding="utf-8", compresslevel=6) as f:
        json.dump(embedded, f, ensure_ascii=False)

    print(f"Saved {len(embedded)} embeddings to {OUTPUT_PATH}")
