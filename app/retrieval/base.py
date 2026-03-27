from abc import ABC, abstractmethod
from typing import List


class BaseRetriever(ABC):
    """
    Abstract retrieval interface.
    Swap implementations (keyword, embedding, vector DB) without
    touching anything outside this module.
    """

    @abstractmethod
    def get_relevant_chunks(self, query: str, k: int = 5) -> List[str]:
        """Return the k most relevant text chunks for the given query."""
        pass
