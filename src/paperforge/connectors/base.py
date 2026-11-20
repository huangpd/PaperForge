from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class SourceRecord:
    source: str
    source_id: str
    doi: str | None
    source_updated_at: datetime | None
    payload: dict  # source-native record


@dataclass(frozen=True)
class WorkUnit:
    """Smallest resumable piece of work (a time window, a domain quota, ...)."""
    key: str
    params: dict = field(default_factory=dict)


@dataclass
class Page:
    records: list[SourceRecord]
    next_cursor: str | None  # resume token for *after* this page; None = unit exhausted


@dataclass(frozen=True)
class RequestStat:
    unit_key: str
    status_code: int | None
    elapsed_ms: float
    attempt: int
    error: str | None = None


OnRequest = Callable[[RequestStat], None]


class SourceConnector(ABC):
    name: str
    # Where the runner keeps the latest source-native record, and which SourceRecord attribute is its key.
    ods_table: str = "ods.crossref_works"
    ods_key: str = "doi_norm"
    key_attr: str = "doi"

    @abstractmethod
    def plan(self, **kwargs) -> list[WorkUnit]:
        """Split a request into resumable units. Must be deterministic for the same input."""

    @abstractmethod
    def fetch(self, unit: WorkUnit, cursor: str | None = None,
              on_request: OnRequest | None = None) -> Iterator[Page]:
        """Yield pages, resuming after `cursor` if given."""
