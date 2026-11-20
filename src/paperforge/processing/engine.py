"""ProcessingEngine: the single processing core used by Batch Mode and Streaming Mode."""
from abc import ABC, abstractmethod
from datetime import UTC, datetime

from paperforge.processing.entity_resolver import resolve
from paperforge.processing.models import Kept, ProcessResult, RawRecord
from paperforge.processing.arxiv import normalize_arxiv
from paperforge.processing.normalizer import normalize_crossref
from paperforge.processing.transformer import transform
from paperforge.processing.validator import errors, validate
from paperforge.repositories.base import PaperRepository, RaceLost

EPOCH = datetime.fromtimestamp(0, UTC)


class ProcessingEngine(ABC):
    @abstractmethod
    def process(self, records: list[RawRecord], quarantine: bool = True) -> ProcessResult: ...


class PolarsEngine(ProcessingEngine):
    """Normalizer -> Validator -> Transformer(Polars) -> EntityResolver -> Repository."""

    def __init__(self, repo: PaperRepository, max_retries: int = 3):
        self.repo, self.max_retries = repo, max_retries

    def process(self, records: list[RawRecord], quarantine: bool = True) -> ProcessResult:
        kept: list[Kept] = []
        bad = []
        for i, r in enumerate(records):
            norm = normalize_arxiv if r.source == "arxiv" else normalize_crossref     # the only per-source switch
            n = norm(r.payload, r.source, r.source_updated_at, r.fallback_ts)
            issues = validate(n)
            errs = errors(issues)
            if errs:
                bad += [(r, e) for e in errs]      # one quarantine row per violated Error rule
                continue
            if n.source_updated_at is None:
                n.source_updated_at = EPOCH
            kept.append(Kept(i, r, n, issues))
        frames = transform([(k.idx, k.n) for k in kept])
        for attempt in range(self.max_retries):
            try:
                res = self.repo.upsert_batch(kept, frames, resolve)
                break
            except RaceLost:
                if attempt == self.max_retries - 1:
                    raise
        quarantined = len({id(r) for r, _ in bad})
        if quarantine:
            self.repo.quarantine(bad)
        res.received = len(records)
        res.quarantined = quarantined
        return res
