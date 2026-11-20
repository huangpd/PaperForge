"""Replay quarantined records after a fix (rule change or corrected payload) through the SAME processing engine."""
from datetime import datetime

import psycopg

from paperforge.processing.engine import ProcessingEngine
from paperforge.processing.models import RawRecord
from paperforge.processing.normalizer import normalize_crossref
from paperforge.processing.validator import errors, validate


def replay_quarantine(conn: psycopg.Connection, engine: ProcessingEngine, rule_id: str | None = None,
                      limit: int | None = None) -> dict[str, int]:
    sql = "SELECT id, source, raw_file_id, raw_line, payload, created_at FROM meta.quarantine_record WHERE status='open'"
    args: list = []
    if rule_id:
        sql += " AND rule_id=%s"; args.append(rule_id)
    sql += " ORDER BY id" + (" LIMIT %s" if limit else "")
    if limit:
        args.append(limit)
    rows = conn.execute(sql, args).fetchall()
    stats = {"examined": len(rows), "replayed": 0, "still_failing": 0}
    done_records: set[tuple] = set()
    for qid, source, fid, line, payload, created in rows:
        n = normalize_crossref(payload, source, None, created)
        errs = errors(validate(n))
        if errs:
            conn.execute("UPDATE meta.quarantine_record SET last_replay_error=%s, rule_id=%s, rule_version=%s, reason=%s WHERE id=%s",
                         (errs[0].message, errs[0].rule_id, errs[0].version, errs[0].message, qid))
            stats["still_failing"] += 1
            continue
        key = (source, fid, line, n.doi)
        if key not in done_records:                      # one record may have several quarantine rows (one per rule)
            engine.process([RawRecord(source, n.doi, n.source_updated_at, payload, created, fid, line)], quarantine=False)
            done_records.add(key)
        conn.execute("UPDATE meta.quarantine_record SET status='replayed', replayed_at=now(), last_replay_error=NULL WHERE id=%s", (qid,))
        stats["replayed"] += 1
    return stats
