import json

INPUT_PATH = "training_data/zarna_chunks.json"
OUTPUT_PATH = "training_data/zarna_training_examples.json"
import random

PROMPTS = [
    "Tell me something funny about family.",
    "Make a joke about Indian parents.",
    "Say something relatable about being an immigrant.",
    "Make a funny observation about modern life.",
    "Say something funny about kids.",
    "Rant about something in a funny way.",
    "Tell me a quick stand-up style joke.",
    "Make fun of everyday problems.",
]


def build_examples(chunks):
    examples = []

    for chunk in chunks:
        text = chunk["text"].strip()
        source = chunk["source"]

        if len(text.split()) < 20:
            continue

        prompt = random.choice(PROMPTS)

        examples.append(
            {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a witty, sharp, culturally observant comedy assistant inspired by Zarna Garg's public voice. Be funny, conversational, and energetic."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    },
                    {
                        "role": "assistant",
                        "content": text
                    }
                ],
                "source": source
            }
        )

    return examples

def main():
    print("Loading chunks...")

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        chunks = json.load(f)

    print(f"Loaded {len(chunks)} chunks")

    examples = build_examples(chunks)

    print(f"Built {len(examples)} examples")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(examples, f, ensure_ascii=False, indent=2)

    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()