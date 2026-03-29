import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from app.config import EMBEDDINGS_PATH
from tests.gemini_test_util import live_gemini_configured

EMBEDDINGS_READY = os.path.exists(EMBEDDINGS_PATH)


@pytest.mark.skipif(not live_gemini_configured(), reason="Needs valid GEMINI_API_KEY for query embeddings")
def test_retrieval():
    if not EMBEDDINGS_READY:
        print(f"⚠️  Embeddings file not found at '{EMBEDDINGS_PATH}'")
        print("   Run: python3 scripts/build_embeddings.py")
        print("   Skipping live retrieval test.")
        return

    from app.retrieval.embedding import EmbeddingRetriever

    retriever = EmbeddingRetriever()

    query = "joke about Indian parents"
    results = retriever.get_relevant_chunks(query, k=3)

    assert isinstance(results, list), "Expected a list"
    assert len(results) == 3, f"Expected 3 chunks, got {len(results)}"
    assert all(isinstance(r, str) and len(r) > 0 for r in results), "Chunks should be non-empty strings"

    print(f"✓ Retrieved {len(results)} chunks for query: '{query}'")
    print("\n--- Top chunk preview ---")
    print(results[0][:200], "...")
    print("\nRetrieval test passed.")


if __name__ == "__main__":
    test_retrieval()
