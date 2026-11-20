from abc import ABC, abstractmethod
from collections.abc import Callable

from paperforge.processing.models import Issue, Kept, ProcessResult, RawRecord
from paperforge.processing.transformer import Frames


class RaceLost(Exception):
    """Another writer created the same paper concurrently; caller retries the chunk."""


class PaperRepository(ABC):
    @abstractmethod
    def upsert_batch(self, kept: list[Kept], frames: Frames, resolver: Callable) -> ProcessResult:
        """Atomically (one transaction): lock existing papers, resolve, write papers/versions/identifiers/children."""

    @abstractmethod
    def quarantine(self, items: list[tuple[RawRecord, Issue]]) -> None: ...
