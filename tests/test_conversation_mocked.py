"""
Multi-turn smoke: no live Gemini or embeddings; verifies brain wiring and prompt content.
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import MagicMock, patch

from app.brain.handler import ZarnaBrain
from app.retrieval.base import BaseRetriever
from app.storage.memory import InMemoryStorage


class _StubRetriever(BaseRetriever):
    def get_relevant_chunks(self, query: str, k: int = 5):
        return [
            "My husband once said he needed a break from the kids. I said great, me too.",
        ]


def test_mocked_multi_turn_conversation_includes_voice_rules_in_prompt():
    phone = "+19998887777"
    user_turns = [
        "tell me a joke about Indian moms",
        "lol true",
        "my mother in law is visiting next week",
        "ugh I love her but she exhausts me",
    ]

    mock_responses = [
        MagicMock(text="Indian moms have three sentences: eat, call back, doctor."),
        MagicMock(text="There it is. If it were easy, nobody would need comedy."),
        MagicMock(text="Next week gives you time to hide the snacks she will judge."),
        MagicMock(
            text="Love and exhaustion in the same breath is the whole marriage skill. "
            "What is your one rule when she is under your roof?"
        ),
    ]

    with patch("app.brain.generator._client.models.generate_content") as mock_gen:
        mock_gen.side_effect = mock_responses

        brain = ZarnaBrain(storage=InMemoryStorage(), retriever=_StubRetriever())

        for msg in user_turns:
            brain.handle_incoming_message(phone, msg)

    assert mock_gen.call_count == len(user_turns)

    first_prompt = mock_gen.call_args_list[0]
    contents = first_prompt.kwargs.get("contents") or first_prompt[0][0]
    blob = contents if isinstance(contents, str) else str(contents)

    assert "Never use" in blob and "my friend" in blob
    assert "No question is the norm" in blob or "Default: land the joke" in blob

    history = brain.storage.get_conversation_history(phone)
    assert len(history) == len(user_turns) * 2
    assert "Indian moms" in history[1].text
