import os
from concurrent.futures import ThreadPoolExecutor

from app.brain.intent import classify_intent
from app.brain.generator import generate_zarna_reply
from app.brain.memory import extract_memory
from app.config import CONVERSATION_HISTORY_LIMIT
from app.retrieval.base import BaseRetriever
from app.storage.base import BaseStorage

# Shared thread pool — reused across requests so we don't pay thread-spawn
# cost on every message. 32 threads handles 100+ simultaneous AI calls
# without queuing (each call is mostly I/O-bound waiting on Gemini).
_executor = ThreadPoolExecutor(max_workers=32)


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

        # 4. Load existing fan memory for personalization
        fan_memory = self.storage.get_memory(phone_number)

        # 5 + 6. Classify intent AND retrieve chunks in parallel.
        future_intent = _executor.submit(classify_intent, message_text)
        future_chunks = _executor.submit(self.retriever.get_relevant_chunks, message_text)

        intent = future_intent.result()
        chunks = future_chunks.result()

        # 7. Generate reply (with fan memory injected)
        reply = generate_zarna_reply(
            intent=intent,
            user_message=message_text,
            chunks=chunks,
            history=history,
            fan_memory=fan_memory,
        )

        # 8. Persist the assistant's reply
        self.storage.save_message(phone_number, "assistant", reply)

        # 9. Update fan memory in the background — no latency impact on reply
        _executor.submit(self._update_memory, phone_number, message_text, fan_memory)

        return reply

    def _update_memory(self, phone_number: str, message_text: str, current_memory: str) -> None:
        try:
            new_memory, new_tags, location = extract_memory(current_memory, message_text)
            if new_memory != current_memory or new_tags or location:
                self.storage.update_memory(phone_number, new_memory, new_tags, location)
        except Exception:
            pass  # Memory update is best-effort; never block a reply


def create_brain() -> ZarnaBrain:
    """
    Factory that wires up the default production dependencies.
    Uses PostgresStorage when DATABASE_URL is set (production on Railway),
    falls back to InMemoryStorage for local dev without a database.
    """
    from app.retrieval.embedding import EmbeddingRetriever

    database_url = os.getenv("DATABASE_URL", "")
    if database_url:
        from app.storage.postgres import PostgresStorage
        # Railway injects postgres:// but psycopg2 requires postgresql://
        dsn = database_url.replace("postgres://", "postgresql://", 1)
        storage = PostgresStorage(dsn=dsn)
    else:
        from app.storage.memory import InMemoryStorage
        storage = InMemoryStorage()

    return ZarnaBrain(
        storage=storage,
        retriever=EmbeddingRetriever(),
    )
