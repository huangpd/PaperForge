"""Deterministic paper-key matching + version-timeline decisions. Pure: no IO, state comes in as arguments."""
import bisect
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime


def uuid7() -> uuid.UUID:
    """Time-ordered UUID (RFC 9562 v7) so paper_id is stable, unique and index-friendly."""
    b = bytearray(int(time.time() * 1000).to_bytes(6, "big") + os.urandom(10))
    b[6] = 0x70 | (b[6] & 0x0F)
    b[8] = 0x80 | (b[8] & 0x3F)
    return uuid.UUID(bytes=bytes(b))


@dataclass
class ExistingPaper:
    paper_id: uuid.UUID
    current_version: int
    current_hash: str
    current_su: datetime
    versions: list[tuple[datetime, int, str]]     # (source_updated_at, version_no, content_hash)


@dataclass
class Incoming:
    rec_idx: int
    key: str                      # paper_key: the DOI, or 'arxiv:<id>' for a paper without one
    source_updated_at: datetime
    content_hash: str


@dataclass
class VersionAdd:
    rec_idx: int
    version_no: int


@dataclass
class PaperDecision:
    key: str
    paper_id: uuid.UUID
    is_new: bool
    adds: list[VersionAdd] = field(default_factory=list)
    current_rec_idx: int | None = None      # record that defines the paper row (new paper, or new current version)
    current_version_no: int | None = None
    refresh_rec_idx: int | None = None      # same content as current, but newer index time -> refresh volatile fields
    outcomes: dict[int, str] = field(default_factory=dict)   # rec_idx -> inserted|updated|late|unchanged|refreshed


def resolve(incoming: list[Incoming], existing: dict[str, ExistingPaper]) -> list[PaperDecision]:
    by_key: dict[str, list[Incoming]] = {}
    for r in incoming:
        by_key.setdefault(r.key, []).append(r)
    out = []
    for key, recs in by_key.items():
        recs = sorted(recs, key=lambda r: (r.source_updated_at, r.rec_idx))   # arrival order breaks ties
        ex = existing.get(key)
        d = PaperDecision(key, ex.paper_id if ex else uuid7(), is_new=ex is None)
        timeline = sorted((su, vno, h) for su, vno, h in ex.versions) if ex else []
        next_no = max((v[1] for v in timeline), default=0) + 1
        cur = (ex.current_su, ex.current_version) if ex else None
        cur_hash = ex.current_hash if ex else None
        for r in recs:
            keys = [(t[0], t[1]) for t in timeline]
            pos = bisect.bisect_right(keys, (r.source_updated_at, float("inf")))
            pred = timeline[pos - 1] if pos else None
            if pred is not None and pred[2] == r.content_hash:
                # No content change relative to what was valid at that time.
                if cur and pred[1] == cur[1] and r.source_updated_at > cur[0] and r.content_hash == cur_hash:
                    d.refresh_rec_idx = r.rec_idx
                    cur = (r.source_updated_at, cur[1])
                    d.outcomes[r.rec_idx] = "refreshed"
                else:
                    d.outcomes[r.rec_idx] = "unchanged"
                continue
            vno, next_no = next_no, next_no + 1
            d.adds.append(VersionAdd(r.rec_idx, vno))
            timeline.insert(pos, (r.source_updated_at, vno, r.content_hash))
            if cur is None or (r.source_updated_at, vno) >= cur:
                cur, cur_hash = (r.source_updated_at, vno), r.content_hash
                d.current_rec_idx, d.current_version_no, d.refresh_rec_idx = r.rec_idx, vno, None
                d.outcomes[r.rec_idx] = "inserted" if d.is_new and len(d.adds) == 1 else "updated"
            else:
                d.outcomes[r.rec_idx] = "late"
        out.append(d)
    return out
