from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class RawRecord:
    """One source-native record as handed to the processing core (batch file line or Kafka event)."""
    source: str
    doi: str | None
    source_updated_at: datetime | None
    payload: dict
    fallback_ts: datetime | None = None      # used only if the payload has no usable update time
    raw_file_id: int | None = None
    raw_line: int | None = None
    source_id: str | None = None             # source-native id (arXiv id); the DOI is not the key for every source


@dataclass
class Issue:
    rule_id: str
    severity: str            # error | warning
    message: str
    version: int = 1


@dataclass
class NormalizedPaper:
    doi: str | None
    source: str
    source_updated_at: datetime | None
    title: str | None = None
    abstract: str | None = None
    container_title: str | None = None
    publisher: str | None = None
    member: str | None = None
    type: str | None = None
    language: str | None = None
    volume: str | None = None
    issue: str | None = None
    page: str | None = None
    issns: list[str] = field(default_factory=list)
    invalid_issns: list[str] = field(default_factory=list)
    issued_year: int | None = None
    issued_month: int | None = None
    issued_day: int | None = None
    issued_invalid: bool = False
    reference_count: int | None = None
    cited_by_count: int | None = None
    authors: list[dict] = field(default_factory=list)
    references: list[dict] = field(default_factory=list)
    content_hash: str = ""
    # Identity. paper_key is the DWD entity key: the DOI when there is one, else 'arxiv:<id>'. None = same as doi (Crossref).
    paper_key: str | None = None
    identifiers: list[tuple[str, str]] = field(default_factory=list)   # (scheme, value) to register for a new paper
    arxiv: dict | None = None                                            # arXiv-only fields (-> dwd.paper_arxiv)

    @property
    def key(self) -> str:
        return self.paper_key or self.doi or ""


@dataclass
class Kept:
    """A record that passed Error-level validation."""
    idx: int
    rec: RawRecord
    n: NormalizedPaper
    issues: list[Issue]


@dataclass
class ProcessResult:
    received: int = 0
    quarantined: int = 0
    inserted: int = 0
    updated: int = 0
    late: int = 0
    refreshed: int = 0
    unchanged: int = 0
    warnings: int = 0

    def add(self, o: "ProcessResult") -> "ProcessResult":
        for k in self.__dataclass_fields__:
            setattr(self, k, getattr(self, k) + getattr(o, k))
        return self
