import json
import math
import os
from dotenv import load_dotenv
load_dotenv()
from google import genai

EMBEDDINGS_PATH = "training_data/zarna_embeddings.json"
EMBEDDING_MODEL = "gemini-embedding-001"

API_KEY = os.getenv("GEMINI_API_KEY", "")
client = genai.Client(api_key=API_KEY)


def load_embeddings():
    with open(EMBEDDINGS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def cosine_similarity(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(x * x for x in b))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def embed_query(text):
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=text,
    )
    return result.embeddings[0].values


def find_top_chunks(user_input, chunks, k=5):
    query_embedding = embed_query(user_input)

    scored = [
        (cosine_similarity(query_embedding, c["embedding"]), c["text"])
        for c in chunks
    ]
    scored.sort(reverse=True)
    return [text for _, text in scored[:k]]


def generate_response(user_input, context_chunks):
    context = "\n\n".join(context_chunks)

    prompt = f"""
You are writing as an AI comedy assistant inspired by Zarna Garg's public comedic voice.

Use these style examples for inspiration:
{context}

Now answer this request:
{user_input}

Write in a voice that feels:
- sharp
- high-energy
- opinionated
- family-centered
- culturally specific when relevant
- conversational, like a stand-up rant or bit

Important rules:
- Do not be generic
- Do not sound like a random comedian
- Do not sound male
- Prefer family, parenting, marriage, immigrant-family, and Indian-mom style angles when relevant
- Keep the response tight — 1 to 2 sentences max
- No setup padding, no preamble, no filler
- Lead with the sharpest line, end on the funniest one
- Do not explain the joke
- Do not copy the source text directly
- Never use the word "honey"

If the user asks for a joke, deliver one punchy one-liner or a two-line bit. That's it.
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    return response.text


def main():
    chunks = load_embeddings()

    user_input = input("You: ")

    top_chunks = find_top_chunks(user_input, chunks)

    print("\n--- Retrieved chunks ---")
    for c in top_chunks:
        print("-", c[:100], "...")

    response = generate_response(user_input, top_chunks)

    print("\nZarna AI response:\n")
    print(response)


if __name__ == "__main__":
    main()