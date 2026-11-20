from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Message:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: dict[str, bytes] = field(default_factory=dict)


@dataclass
class Outgoing:
    topic: str
    key: bytes
    value: bytes
    headers: dict[str, bytes] = field(default_factory=dict)


class EventConsumer(ABC):
    @abstractmethod
    def poll_batch(self, max_messages: int, timeout_s: float) -> list[Message]:
        """Return up to max_messages (possibly none). Never commits."""

    @abstractmethod
    def commit(self, messages: list[Message]) -> None:
        """Synchronously commit progress up to and including these messages (offset+1 per partition)."""

    @abstractmethod
    def close(self) -> None: ...


class EventBus(ABC):
    @abstractmethod
    def publish(self, items: list[Outgoing]) -> list[Exception | None]:
        """Send and wait for broker acknowledgement. Returns one entry per item: None = acked, Exception = failed.
        An ack means *the broker has the message* - it does NOT mean the paper was processed."""

    @abstractmethod
    def consumer(self, group: str, topics: list[str]) -> EventConsumer:
        """New groups start from the earliest offset (so a fresh group name == full replay)."""

    @abstractmethod
    def ensure_topics(self, topics: dict[str, int]) -> None: ...

    @abstractmethod
    def lag(self, group: str, topic: str) -> dict[int, tuple[int, int]]:
        """partition -> (committed_offset_or_-1, end_offset)."""
