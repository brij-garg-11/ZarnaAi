from app.brain.intent import classify_intent
from app.brain.generator import generate_zarna_reply
from app.config import CONVERSATION_HISTORY_LIMIT
from app.retrieval.base import BaseRetriever
from app.storage.base import BaseStorage


class ZarnaBrain:
    """
    Central handler. Owns no state of its own — all persistence goes through
    storage, all content retrieval goes through retriever. Swap either without
    touching this class.
    """

    def __init__(self, storage: BaseStorage, retriever: BaseRetriever):
        self.storage = storage
        self.retriever = retriever

    def handle_incoming_message(self, phone_number: str, message_text: str) -> str:
        # 1. Ensure contact exists
        self.storage.save_contact(phone_number)

        # 2. Persist the user's message
        self.storage.save_message(phone_number, "user", message_text)

        # 3. Pull prior conversation (excluding the message we just saved)
        raw_history = self.storage.get_conversation_history(
            phone_number, limit=CONVERSATION_HISTORY_LIMIT + 1
        )
        history = [{"role": m.role, "text": m.text} for m in raw_history[:-1]]

        # 4. Classify intent
        intent = classify_intent(message_text)

        # 5. Retrieve relevant chunks
        chunks = self.retriever.get_relevant_chunks(message_text)

        # 6. Generate reply
        reply = generate_zarna_reply(
            intent=intent,
            user_message=message_text,
            chunks=chunks,
            history=history,
        )

        # 7. Persist the assistant's reply
        self.storage.save_message(phone_number, "assistant", reply)

        return reply


def create_brain() -> ZarnaBrain:
    """
    Factory that wires up the default production dependencies.
    The Flask app (and any future entry point) calls this once at startup.
    """
    from app.storage.memory import InMemoryStorage
    from app.retrieval.embedding import EmbeddingRetriever

    return ZarnaBrain(
        storage=InMemoryStorage(),
        retriever=EmbeddingRetriever(),
    )
