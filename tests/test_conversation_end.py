import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.brain.conversation_end import is_conversation_ender


def test_lol_and_reactions_are_enders():
    assert is_conversation_ender("lol")
    assert is_conversation_ender("LOL!")
    assert is_conversation_ender("haha")
    assert is_conversation_ender("lmao")
    assert is_conversation_ender("thanks")
    assert is_conversation_ender("thank you")
    assert is_conversation_ender("ok")
    assert is_conversation_ender("okay")
    assert is_conversation_ender("kk")
    assert is_conversation_ender("ty")
    assert is_conversation_ender("np")


def test_substantive_messages_not_enders():
    assert not is_conversation_ender("lol that was so funny tell me another")
    assert not is_conversation_ender("I'm feeling sad")
    assert not is_conversation_ender("buy milk on the way home")
    assert not is_conversation_ender("ok but what about taxes")


def test_ambiguous_one_word_acks_get_replies():
    """nice/cool alone used to skip; now only strict closers end the thread."""
    assert not is_conversation_ender("nice")
    assert not is_conversation_ender("cool")
    assert not is_conversation_ender("awesome")
    assert not is_conversation_ender("yep")


def test_long_thanks_still_gets_reply():
    assert not is_conversation_ender("thank you for the show last night it meant a lot")


def test_short_strict_still_enders():
    assert is_conversation_ender("thank you so much")
    assert is_conversation_ender("ty!!")


def test_handler_skips_reply_for_lol():
    from unittest.mock import patch, MagicMock
    from app.brain.handler import ZarnaBrain
    from app.storage.memory import InMemoryStorage
    from app.retrieval.base import BaseRetriever

    class R(BaseRetriever):
        def get_relevant_chunks(self, query: str, k: int = 5):
            return []

    brain = ZarnaBrain(InMemoryStorage(), R())
    with patch("app.brain.handler.generate_zarna_reply", MagicMock()) as mock_gen:
        out = brain.handle_incoming_message("+19995550123", "lol")
        assert out == ""
        mock_gen.assert_not_called()
