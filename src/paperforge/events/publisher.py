"""Producers of paper.raw.events.
- publish_outbox (optionally only the given DOIs): ODS rows not yet announced (published_at IS NULL) -> events. published_at is set only after the broker ack,
  and only for the exact payload_hash that was sent (a row updated meanwhile stays pending).
- publish_raw_files: replay immutable raw JSONL files (history, including superseded versions) as events."""
import json
import uuid
from datetime import datetime

from paperforge.db import connect
from paperforge.events.bus import EventBus, Outgoing
from paperforge.events.schema import Event, make_event
from paperforge.processing.normalizer import raw_payload_hash
from paperforge.storage.base import RawStorage


def _outgoing(topic: str, ev: Event, file_id: int | None, line: int | None) -> Outgoing:
    h = {"event_type": ev.event_type.encode()}
    if file_id is not None:
        h["raw_file_id"], h["raw_line"] = str(file_id).encode(), str(line).encode()
    return Outgoing(topic, ev.key(), ev.to_bytes(), h)


def publish_outbox(bus: EventBus, topic: str, dsn: str | None = None, limit: int | None = None,
                   chunk: int = 2000, trace_id: str | None = None, dois: list[str] | None = None) -> dict:
    trace_id = trace_id or f"outbox-{uuid.uuid4().hex[:12]}"
    sent = failed = 0
    with connect(dsn) as c:
        remaining = limit
        while remaining is None or remaining > 0:
            n = chunk if remaining is None else min(chunk, remaining)
            rows = c.execute("""SELECT doi_norm, source, payload, payload_hash, source_updated_at, ingested_at, raw_file_id, raw_line
                                FROM ods.crossref_works WHERE published_at IS NULL AND (%s::text[] IS NULL OR doi_norm = ANY(%s))
                                ORDER BY ingested_at, doi_norm LIMIT %s""", (dois, dois, n)).fetchall()
            if not rows:
                break
            items, meta = [], []
            for doi, source, payload, ph, su, ing, fid, line in rows:
                ev = make_event(source, doi, payload, ph, su, trace_id, ing)
                items.append(_outgoing(topic, ev, fid, line)); meta.append((doi, ph))
            res = bus.publish(items)
            ok = [m for m, r in zip(meta, res) if r is None]
            failed += len(res) - len(ok)
            if ok:
                c.execute("""UPDATE ods.crossref_works w SET published_at = now()
                             FROM unnest(%s::text[], %s::text[]) AS t(doi, h) WHERE w.doi_norm = t.doi AND w.payload_hash = t.h""",
                          ([d for d, _ in ok], [h for _, h in ok]))
            sent += len(ok)
            if failed:       # stop on first failing chunk: the same rows would be selected again forever otherwise
                break
            remaining = None if remaining is None else remaining - len(rows)
    return {"published": sent, "failed": failed, "trace_id": trace_id}


def publish_raw_files(bus: EventBus, topic: str, storage: RawStorage, dsn: str | None = None, batch_id: str | None = None,
                      chunk: int = 2000) -> dict:
    """Replay raw files in arrival order. Event ids are deterministic, so unchanged content is idempotent downstream."""
    trace_id = f"replay-{batch_id or 'all'}-{uuid.uuid4().hex[:8]}"
    with connect(dsn) as c:
        files = c.execute("SELECT file_id, path, created_at FROM ods.raw_file_manifest" + (" WHERE batch_id=%s" if batch_id else "")
                          + " ORDER BY file_id", (batch_id,) if batch_id else None).fetchall()
    sent = failed = 0
    for fid, path, created in files:
        items = []
        for line_no, line in enumerate(storage.read(path).splitlines()):
            o = json.loads(line)
            su = datetime.fromisoformat(o["source_updated_at"]) if o.get("source_updated_at") else None
            ev = make_event(o["source"], o.get("doi"), o["payload"], raw_payload_hash(o["payload"]), su, trace_id, created,
                            source_id=o.get("source_id"))
            items.append(_outgoing(topic, ev, fid, line_no))
        for i in range(0, len(items), chunk):
            res = bus.publish(items[i:i + chunk])
            sent += sum(r is None for r in res); failed += sum(r is not None for r in res)
    return {"published": sent, "failed": failed, "files": len(files), "trace_id": trace_id}
