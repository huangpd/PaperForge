"""Ingestion runner: connector pages -> immutable JSONL -> manifest + ODS + checkpoint (one PG txn per page)."""
import json
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime

import psycopg
from psycopg.types.json import Jsonb

from paperforge.connectors.base import RequestStat, SourceConnector, SourceRecord, WorkUnit
from paperforge.db import connect
from paperforge.processing.normalizer import raw_payload_hash
from paperforge.storage.base import RawStorage

log = logging.getLogger(__name__)

UPSERT = """
INSERT INTO {table}
  ({key}, source, source_id, payload, payload_hash, source_updated_at, batch_id, raw_file_id, raw_line)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ({key}) DO UPDATE SET
  payload = EXCLUDED.payload, payload_hash = EXCLUDED.payload_hash,
  source_updated_at = EXCLUDED.source_updated_at, batch_id = EXCLUDED.batch_id,
  raw_file_id = EXCLUDED.raw_file_id, raw_line = EXCLUDED.raw_line, ingested_at = now(), published_at = NULL
WHERE {table}.payload_hash <> EXCLUDED.payload_hash
  AND ({table}.source_updated_at IS NULL OR EXCLUDED.source_updated_at IS NULL
       OR EXCLUDED.source_updated_at >= {table}.source_updated_at)
RETURNING (xmax = 0)
"""


class LimitReached(Exception):
    pass


@dataclass
class RunResult:
    batch_id: str
    status: str
    rows_seen: int
    rows_inserted: int
    rows_updated: int
    rows_unchanged: int
    rows_skipped: int
    units_done: int
    units_failed: int
    truncated: bool = False  # stopped early by the global limit: not all windows were fully fetched


def _line(rec: SourceRecord) -> bytes:
    obj = {"source": rec.source, "source_id": rec.source_id, "doi": rec.doi,
           "source_updated_at": rec.source_updated_at.isoformat() if rec.source_updated_at else None,
           "payload": rec.payload}
    # \u0000 cannot be stored in jsonb; drop it so raw file and ODS stay identical.
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).replace("\\u0000", "").encode() + b"\n"


class IngestionRunner:
    def __init__(self, connector: SourceConnector, storage: RawStorage, dsn: str | None = None, concurrency: int = 1):
        self.connector, self.storage, self.dsn, self.concurrency = connector, storage, dsn, concurrency
        self._lock = threading.Lock()
        self._seen = 0

    # ---- batch / checkpoint bookkeeping -------------------------------------------------
    def start(self, mode: str, units: list[WorkUnit], plan_params: dict, limit: int | None,
              batch_id: str | None = None) -> str:
        """Create a batch (or reopen an unfinished one when batch_id exists)."""
        with connect(self.dsn) as c:
            if batch_id:
                row = c.execute("SELECT status, params FROM ods.ingestion_batch WHERE batch_id=%s", (batch_id,)).fetchone()
                if not row:
                    raise ValueError(f"unknown batch {batch_id}")
                if row[0] == "completed" and not row[1].get("truncated"):
                    raise ValueError(f"batch {batch_id} already completed")
                c.execute("UPDATE ods.ingestion_batch SET status='running', error=NULL WHERE batch_id=%s", (batch_id,))
                return batch_id
            batch_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
            params = {"limit": limit, "date": datetime.now(UTC).date().isoformat(), "plan": plan_params,
                      "units": [{"key": u.key, "params": u.params} for u in units]}
            with c.transaction():
                c.execute("INSERT INTO ods.ingestion_batch(batch_id,source,mode,status,params) VALUES (%s,%s,%s,'running',%s)",
                          (batch_id, self.connector.name, mode, Jsonb(params)))
                for u in units:
                    c.execute("INSERT INTO meta.sync_checkpoint(source,batch_id,unit_key,status,window_from,window_until) "
                              "VALUES (%s,%s,%s,'pending',%s,%s)",
                              (self.connector.name, batch_id, u.key, u.params.get("from"), u.params.get("until")))
            return batch_id

    def run(self, batch_id: str) -> RunResult:
        with connect(self.dsn) as c:
            params = c.execute("SELECT params FROM ods.ingestion_batch WHERE batch_id=%s", (batch_id,)).fetchone()[0]
            done = c.execute("SELECT coalesce(sum(rows_seen),0) FROM meta.sync_checkpoint WHERE batch_id=%s", (batch_id,)).fetchone()[0]
        self._seen = int(done)
        limit = params.get("limit")  # global cap across units (API sync); parquet bootstrap encodes limit as per-unit quotas
        units = [(i, WorkUnit(u["key"], u["params"])) for i, u in enumerate(params["units"])]
        errors = []

        def work(item):
            i, u = item
            try:
                self._run_unit(batch_id, params["date"], i, u, limit)
            except LimitReached:
                pass
            except Exception as e:  # noqa: BLE001 - recorded in checkpoint, batch becomes 'failed'
                log.exception("unit %s failed", u.key)
                errors.append((u.key, repr(e)))

        with ThreadPoolExecutor(max_workers=max(1, self.concurrency)) as ex:
            list(ex.map(work, units))
        return self._finish(batch_id, errors)

    def _finish(self, batch_id: str, errors: list) -> RunResult:
        with connect(self.dsn) as c:
            s = c.execute("""SELECT coalesce(sum(rows_seen),0), coalesce(sum(rows_inserted),0), coalesce(sum(rows_updated),0),
                                    coalesce(sum(rows_unchanged),0), coalesce(sum(rows_skipped),0),
                                    count(*) FILTER (WHERE status='done'), count(*) FILTER (WHERE status='failed')
                             FROM meta.sync_checkpoint WHERE batch_id=%s""", (batch_id,)).fetchone()
            pending = c.execute("SELECT count(*) FROM meta.sync_checkpoint WHERE batch_id=%s AND status<>'done'", (batch_id,)).fetchone()[0]
            lim = c.execute("SELECT (params->>'limit')::bigint FROM ods.ingestion_batch WHERE batch_id=%s", (batch_id,)).fetchone()[0]
            truncated = bool(pending and lim and s[0] >= lim and not errors)
            status = "failed" if (errors or (pending and not truncated)) else "completed"
            if truncated:
                c.execute("UPDATE ods.ingestion_batch SET params = params || '{\"truncated\": true}' WHERE batch_id=%s", (batch_id,))
            c.execute("""UPDATE ods.ingestion_batch SET status=%s, finished_at=now(), error=%s, rows_seen=%s, rows_inserted=%s,
                         rows_updated=%s, rows_unchanged=%s, rows_skipped=%s WHERE batch_id=%s""",
                      (status, json.dumps(errors) if errors else None, *s[:5], batch_id))
        return RunResult(batch_id, status, *[int(x) for x in s], truncated=truncated)

    # ---- one unit -------------------------------------------------------------------------
    def _run_unit(self, batch_id: str, bdate: str, idx: int, unit: WorkUnit, limit: int | None):
        c = connect(self.dsn)
        try:
            row = c.execute("SELECT status, cursor, parts_done FROM meta.sync_checkpoint WHERE source=%s AND batch_id=%s AND unit_key=%s",
                            (self.connector.name, batch_id, unit.key)).fetchone()
            status, cursor, parts = row
            if status == "done":
                return
            key = (self.connector.name, batch_id, unit.key)
            c.execute("UPDATE meta.sync_checkpoint SET status='running', attempt=attempt+1, error=NULL, updated_at=now() "
                      "WHERE source=%s AND batch_id=%s AND unit_key=%s", key)

            def on_req(s: RequestStat):
                c2 = connect(self.dsn)
                try:
                    c2.execute("INSERT INTO ods.ingestion_request(batch_id,unit_key,status_code,elapsed_ms,attempt,error) VALUES (%s,%s,%s,%s,%s,%s)",
                               (batch_id, s.unit_key, s.status_code, s.elapsed_ms, s.attempt, s.error))
                finally:
                    c2.close()

            truncated = False
            try:
                for page in self.connector.fetch(unit, cursor, on_req):
                    recs = page.records
                    next_cursor = page.next_cursor
                    capped = False
                    if limit is not None:
                        with self._lock:
                            room = limit - self._seen
                            if room <= 0:
                                truncated = True
                                break
                            capped = len(recs) >= room
                            if capped:
                                recs, next_cursor = recs[:room], None
                            self._seen += len(recs)
                    parts = self._persist(c, batch_id, bdate, idx, unit, parts, recs, next_cursor)
                    cursor = next_cursor
                    if limit is not None and capped:
                        truncated = True
                        break  # do not request another page once the cap is reached
                # A window cut short by the limit is NOT done: it must never count as fully synced.
                c.execute("UPDATE meta.sync_checkpoint SET status=%s, cursor=NULL, updated_at=now() "
                          "WHERE source=%s AND batch_id=%s AND unit_key=%s", ("pending" if truncated else "done", *key))
            except Exception as e:
                c.execute("UPDATE meta.sync_checkpoint SET status='failed', error=%s, updated_at=now() "
                          "WHERE source=%s AND batch_id=%s AND unit_key=%s", (repr(e)[:1000], *key))
                raise
        finally:
            c.close()

    def _persist(self, c: psycopg.Connection, batch_id, bdate, idx, unit, parts, recs, next_cursor) -> int:
        valid = [r for r in recs if getattr(r, self.connector.key_attr)]
        upsert = UPSERT.format(table=self.connector.ods_table, key=self.connector.ods_key)
        skipped = len(recs) - len(valid)
        seq = parts + 1
        path = f"raw/source={self.connector.name}/date={bdate}/batch_id={batch_id}/part-{idx:03d}-{seq:04d}.jsonl"
        # A file at this path can only be an orphan (written, but its PG txn never committed): seq comes from the checkpoint.
        info = self.storage.write(path, b"".join(_line(r) for r in valid), overwrite=True) if valid else None
        ins = upd = 0
        with c.transaction():
            file_id = None
            if info:
                file_id = c.execute("INSERT INTO ods.raw_file_manifest(batch_id,unit_key,seq,path,record_count,size_bytes,sha256) "
                                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING file_id",
                                    (batch_id, unit.key, seq, info.path, len(valid), info.size_bytes, info.sha256)).fetchone()[0]
                with c.cursor() as cur:
                    for line, r in enumerate(valid):
                        cur.execute(upsert, (getattr(r, self.connector.key_attr), r.source, r.source_id, Jsonb(r.payload), raw_payload_hash(r.payload),
                                             r.source_updated_at, batch_id, file_id, line))
                        res = cur.fetchone()
                        if res is not None:
                            ins, upd = ins + bool(res[0]), upd + (not res[0])
            unchanged = len(valid) - ins - upd
            c.execute("""UPDATE meta.sync_checkpoint SET cursor=%s, parts_done=%s, rows_seen=rows_seen+%s,
                         rows_inserted=rows_inserted+%s, rows_updated=rows_updated+%s, rows_unchanged=rows_unchanged+%s,
                         rows_skipped=rows_skipped+%s, updated_at=now()
                         WHERE source=%s AND batch_id=%s AND unit_key=%s""",
                      (next_cursor, seq if info else parts, len(recs), ins, upd, unchanged, skipped,
                       self.connector.name, batch_id, unit.key))
        return seq if info else parts
