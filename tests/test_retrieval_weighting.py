import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_source_weighting_prefers_facts_over_podcast_transcripts():
    from app.retrieval.embedding import EmbeddingRetriever

    with patch.object(EmbeddingRetriever, "_load"), patch.object(
        EmbeddingRetriever, "_load_podcast_transcript_sources"
    ):
        retriever = EmbeddingRetriever()

    retriever._chunks = [
        {
            "text": "podcast transcript candidate",
            "source": "XCggmEzjvHo_transcript.json",
            "embedding": [1.0, 0.0],
        },
        {
            "text": "canonical fact candidate",
            "source": "zarna_facts",
            "embedding": [1.0, 0.0],
        },
        {
            "text": "book candidate",
            "source": "this american woman.pdf",
            "embedding": [1.0, 0.0],
        },
    ]
    retriever._podcast_transcript_sources = {"xcggmezjvho_transcript.json"}

    with patch.object(EmbeddingRetriever, "_embed", return_value=[1.0, 0.0]):
        out = retriever._cached_search("family facts", 3)

    assert out[0] == "canonical fact candidate"
    assert out[1] == "book candidate"
    assert out[2] == "podcast transcript candidate"


def test_exclude_mode_filters_podcast_transcript_chunks_on_load():
    from app.retrieval.embedding import EmbeddingRetriever

    fake_chunks = [
        {"text": "podcast transcript", "source": "XCggmEzjvHo_transcript.json", "embedding": [1.0]},
        {"text": "fact", "source": "zarna_facts", "embedding": [1.0]},
    ]

    with patch("app.retrieval.embedding.PODCAST_TRANSCRIPTS_MODE", "exclude"), patch.object(
        EmbeddingRetriever, "_load_podcast_transcript_sources"
    ), patch("app.retrieval.embedding.gzip.open") as gzopen:
        gzopen.return_value.__enter__.return_value = object()
        with patch("app.retrieval.embedding.json.load", return_value=fake_chunks):
            retriever = EmbeddingRetriever()

    retriever._podcast_transcript_sources = {"xcggmezjvho_transcript.json"}
    remaining = [c["source"] for c in retriever._chunks]
    assert "zarna_facts" in remaining
    assert "XCggmEzjvHo_transcript.json" not in remaining
