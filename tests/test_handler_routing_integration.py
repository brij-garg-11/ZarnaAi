"""Handler calls routing for GENERAL/JOKE, not for structured intents."""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@patch("app.brain.handler.extract_memory", return_value=("", [], "", False))
@patch("app.brain.handler.generate_zarna_reply")
@patch("app.brain.handler.classify_routing_tier")
@patch("app.brain.handler.classify_intent")
def test_general_message_invokes_classifier(mock_intent, mock_route, mock_gen, _mem):
    mock_intent.return_value = __import__("app.brain.intent", fromlist=["Intent"]).Intent.GENERAL
    mock_route.return_value = "medium"
    mock_gen.return_value = "reply"

    from app.brain.handler import ZarnaBrain
    from app.storage.memory import InMemoryStorage

    retriever = MagicMock()
    retriever.get_relevant_chunks.return_value = []
    brain = ZarnaBrain(storage=InMemoryStorage(), retriever=retriever)

    out = brain.handle_incoming_message("+15550001", "Tell me about Zarna.")
    assert out == "reply"
    mock_route.assert_called_once()
    mock_gen.assert_called_once()
    call_kw = mock_gen.call_args[1]
    assert call_kw.get("routing_tier") == "medium"


@patch("app.brain.handler.extract_memory", return_value=("", [], "", False))
@patch("app.brain.handler.generate_zarna_reply")
@patch("app.brain.handler.classify_routing_tier")
@patch("app.brain.handler.classify_intent")
def test_show_intent_skips_classifier(mock_intent, mock_route, mock_gen, _mem):
    Intent = __import__("app.brain.intent", fromlist=["Intent"]).Intent
    mock_intent.return_value = Intent.SHOW
    mock_gen.return_value = "tickets\nhttps://zarnagarg.com/tickets/"

    from app.brain.handler import ZarnaBrain
    from app.storage.memory import InMemoryStorage

    retriever = MagicMock()
    retriever.get_relevant_chunks.return_value = []
    brain = ZarnaBrain(storage=InMemoryStorage(), retriever=retriever)

    brain.handle_incoming_message("+15550002", "When is the tour?")
    mock_route.assert_not_called()
    call_kw = mock_gen.call_args[1]
    assert call_kw.get("routing_tier") is None
