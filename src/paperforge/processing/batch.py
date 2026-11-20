"""Batch Mode: replay raw JSONL files (via manifest) through the shared ProcessingEngine."""
import json
from collections.abc import Iterator
from datetime import datetime

from paperforge.db import connect
from paperforge.processing.engine import ProcessingEngine
from paperforge.processing.models import ProcessResult, RawRecord
from paperforge.storage.base import RawStorage


def iter_raw_records(storage: RawStorage, dsn: str | None = None, batch_id: str | None = None,
                     file_ids: list[int] | None = None) -> Iterator[tuple[int, list[RawRecord]]]:
    """Yield (file_id, records) per raw file in arrival (file_id) order."""
    sql = "SELECT file_id, path, created_at FROM ods.raw_file_manifest"
    cond, args = [], []
    if batch_id:
        cond.append("batch_id=%s"); args.append(batch_id)
    if file_ids:
        cond.append("file_id = ANY(%s)"); args.append(file_ids)
    with connect(dsn) as c:
        files = c.execute(sql + (" WHERE " + " AND ".join(cond) if cond else "") + " ORDER BY file_id", args).fetchall()
    for file_id, path, created_at in files:
        recs = []
        for line_no, line in enumerate(storage.read(path).splitlines()):
            o = json.loads(line)
            su = datetime.fromisoformat(o["source_updated_at"]) if o.get("source_updated_at") else None
            recs.append(RawRecord(o["source"], o.get("doi"), su, o["payload"], created_at, file_id, line_no, o.get("source_id")))
        yield file_id, recs


def run_batch(engine: ProcessingEngine, storage: RawStorage, dsn: str | None = None, batch_id: str | None = None,
              chunk_size: int = 1000, progress=None) -> ProcessResult:
    total = ProcessResult()
    for file_id, recs in iter_raw_records(storage, dsn, batch_id):
        for i in range(0, len(recs), chunk_size):
            total.add(engine.process(recs[i:i + chunk_size]))
        if progress:
            progress(file_id, total)
    return total
