import gzip
import json
import math
from typing import List

from google import genai

from app.config import GEMINI_API_KEY, EMBEDDING_MODEL, EMBEDDINGS_PATH, TOP_K_CHUNKS
from app.retrieval.base import BaseRetriever


class EmbeddingRetriever(BaseRetriever):
    """
    Retrieves chunks using cosine similarity over pre-built Gemini embeddings.
    Loads the embeddings file once on first use (lazy load).
    """

    def __init__(self, embeddings_path: str = EMBEDDINGS_PATH):
        self._path = embeddings_path
        self._chunks = None
        self._client = genai.Client(api_key=GEMINI_API_KEY)

    def _load(self):
        if self._chunks is None:
            open_fn = gzip.open if self._path.endswith(".gz") else open
            with open_fn(self._path, "rt", encoding="utf-8") as f:
                self._chunks = json.load(f)

    def _embed(self, text: str) -> List[float]:
        result = self._client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=text,
        )
        return result.embeddings[0].values

    @staticmethod
    def _cosine_similarity(a: List[float], b: List[float]) -> float:
        dot = sum(x * y for x, y in zip(a, b))
        mag_a = math.sqrt(sum(x * x for x in a))
        mag_b = math.sqrt(sum(x * x for x in b))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    def get_relevant_chunks(self, query: str, k: int = TOP_K_CHUNKS) -> List[str]:
        self._load()
        query_embedding = self._embed(query)

        scored = [
            (self._cosine_similarity(query_embedding, c["embedding"]), c["text"])
            for c in self._chunks
        ]
        scored.sort(reverse=True)
        return [text for _, text in scored[:k]]
