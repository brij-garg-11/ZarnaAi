import json
import time
from google import genai

INPUT_PATH = "training_data/zarna_chunks.json"
OUTPUT_PATH = "training_data/zarna_embeddings.json"

API_KEY = "AIzaSyASvhhRlQWODFz35C3r1sSScQZTAME1uz8"
EMBEDDING_MODEL = "gemini-embedding-001"
BATCH_SIZE = 50

client = genai.Client(api_key=API_KEY)


def embed_chunks(chunks):
    embedded = []
    total = len(chunks)

    for i in range(0, total, BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        texts = [c["text"] for c in batch]

        result = client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=texts,
        )

        for j, emb in enumerate(result.embeddings):
            embedded.append({
                "text": batch[j]["text"],
                "source": batch[j]["source"],
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

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(embedded, f, ensure_ascii=False)

    print(f"Saved {len(embedded)} embeddings to {OUTPUT_PATH}")
