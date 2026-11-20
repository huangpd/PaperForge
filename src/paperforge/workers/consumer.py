"""Streaming Mode worker.

Guarantees (at-least-once + idempotent writes):
  poll micro-batch -> ProcessingEngine.process() (one DB transaction per chunk, the SAME core as Batch Mode)
  -> [failed events -> paper.dlq, acked] -> commit offsets.
Offsets are committed only after the data is durable in PostgreSQL. A crash anywhere before the commit causes redelivery;
redelivered events are no-ops because versions are keyed by (paper, source_updated_at, content_hash)."""
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import psycopg

from paperforge.events.bus import EventBus, Message, Outgoing
from paperforge.events.schema import SCHEMA_VERSION, SUPPORTED_TYPES, Event, dlq_message
from paperforge.processing.engine import ProcessingEngine
from paperforge.processing.models import ProcessResult, RawRecord

log = logging.getLogger(__name__)
TRANSIENT = (psycopg.OperationalError, psycopg.InterfaceError)


@dataclass
class WorkerStats:
    batches: int = 0
    messages: int = 0
    dlq: int = 0
    result: ProcessResult = field(default_factory=ProcessResult)


def to_record(ev: Event, headers: dict[str, bytes]) -> RawRecord:
    fid = headers.get("raw_file_id")
    line = headers.get("raw_line")
    return RawRecord(ev.source, ev.doi, ev.source_updated_at, ev.payload, ev.ingested_at,
                     int(fid) if fid else None, int(line) if line else None)


class StreamWorker:
    def __init__(self, bus: EventBus, engine: ProcessingEngine, group: str, topic: str, dlq_topic: str,
                 batch_size: int = 500, batch_timeout_s: float = 2.0, max_retries: int = 3,
                 retry_backoff_s: float = 0.5, on_reconnect: Callable[[], None] | None = None,
                 before_commit: Callable[[list[Message]], None] | None = None):
        self.bus, self.engine, self.group, self.topic, self.dlq_topic = bus, engine, group, topic, dlq_topic
        self.batch_size, self.batch_timeout_s, self.max_retries = batch_size, batch_timeout_s, max_retries
        self.retry_backoff_s, self.on_reconnect = retry_backoff_s, on_reconnect
        self.before_commit = before_commit        # fault-injection / test hook; production leaves it None
        self.consumer = bus.consumer(group, [topic])
        self.stats = WorkerStats()

    # -- DLQ ------------------------------------------------------------------------------------
    def _to_dlq(self, m: Message, error_type: str, error: str, attempts: int) -> Outgoing:
        return Outgoing(self.dlq_topic, m.key or b"", dlq_message(m.value, m.topic, m.partition, m.offset, self.group,
                                                                  error_type, error, attempts, m.headers), {"error_type": error_type.encode()})

    def _send_dlq(self, items: list[Outgoing]) -> None:
        if not items:
            return
        res = self.bus.publish(items)
        bad = [r for r in res if r is not None]
        if bad:                                   # cannot persist the failure -> do NOT commit; let the batch be redelivered
            raise RuntimeError(f"DLQ publish failed: {bad[0]}")
        self.stats.dlq += len(items)

    # -- processing -----------------------------------------------------------------------------
    def _process(self, recs: list[RawRecord]) -> ProcessResult:
        delay = self.retry_backoff_s
        for attempt in range(1, self.max_retries + 1):
            try:
                return self.engine.process(recs)
            except TRANSIENT as e:                # database hiccup: not the event's fault, retry, never DLQ
                log.warning("transient DB error (attempt %s): %s", attempt, e)
                if attempt == self.max_retries:
                    raise
                if self.on_reconnect:
                    self.on_reconnect()
                time.sleep(delay); delay *= 2
        raise AssertionError("unreachable")

    def step(self) -> int:
        msgs = self.consumer.poll_batch(self.batch_size, self.batch_timeout_s)
        if not msgs:
            return 0
        good: list[tuple[Message, Event]] = []
        dlq: list[Outgoing] = []
        for m in msgs:
            try:
                ev = Event.from_bytes(m.value or b"")
                if ev.schema_version > SCHEMA_VERSION or ev.event_type not in SUPPORTED_TYPES:
                    raise ValueError(f"unsupported event (type={ev.event_type}, schema_version={ev.schema_version})")
                good.append((m, ev))
            except Exception as e:  # noqa: BLE001 - poison message: parse/validation failure
                dlq.append(self._to_dlq(m, "invalid_event", repr(e), 1))
        res = ProcessResult()
        if good:
            recs = [to_record(ev, m.headers) for m, ev in good]
            try:
                res = self._process(recs)
            except TRANSIENT:
                raise                              # exhausted retries: crash without committing; supervisor restarts us
            except Exception as e:  # noqa: BLE001
                log.warning("batch failed (%r); isolating the poison event(s) one by one", e)
                res = ProcessResult()
                for (m, _ev), rec in zip(good, recs):
                    try:
                        res.add(self._process([rec]))
                    except TRANSIENT:
                        raise
                    except Exception as e2:  # noqa: BLE001
                        dlq.append(self._to_dlq(m, type(e2).__name__, repr(e2), self.max_retries))
        self._send_dlq(dlq)                        # failures are durable in the DLQ ...
        if self.before_commit:
            self.before_commit(msgs)
        self.consumer.commit(msgs)                 # ... and only now is progress committed
        self.stats.batches += 1
        self.stats.messages += len(msgs)
        self.stats.result.add(res)
        return len(msgs)

    def run(self, should_stop: Callable[[], bool] = lambda: False, idle_exit_s: float | None = None) -> WorkerStats:
        idle_since = time.monotonic()
        try:
            while not should_stop():
                if self.step():
                    idle_since = time.monotonic()
                elif idle_exit_s is not None and time.monotonic() - idle_since >= idle_exit_s:
                    break
        finally:
            self.consumer.close()
        return self.stats


def replay_dlq(bus: EventBus, dlq_topic: str, raw_topic: str, group: str, idle_exit_s: float = 5.0) -> dict:
    """Move DLQ'd events back to the raw topic after the cause was fixed. Unparseable originals stay in the DLQ (reported)."""
    import json
    cons = bus.consumer(group, [dlq_topic])
    moved = skipped = 0
    idle = time.monotonic()
    try:
        while time.monotonic() - idle < idle_exit_s:
            msgs = cons.poll_batch(200, 1.0)
            if not msgs:
                continue
            idle = time.monotonic()
            out = []
            for m in msgs:
                try:
                    d = json.loads(m.value)
                    ev = d["event"]
                    e = Event.model_validate(ev)
                    if e.schema_version > SCHEMA_VERSION or e.event_type not in SUPPORTED_TYPES:
                        raise ValueError("not processable by this worker version; upgrade first")
                    hdr = {k: v.encode() for k, v in (d.get("headers") or {}).items() if k.startswith("raw_")}
                    out.append(Outgoing(raw_topic, m.key or b"", json.dumps(ev, ensure_ascii=False).encode(),
                                        {**hdr, "replayed_from_dlq": b"1"}))
                except Exception:  # noqa: BLE001
                    skipped += 1
            if out:
                if any(r is not None for r in bus.publish(out)):
                    raise RuntimeError("republish failed; offsets not committed")
                moved += len(out)
            cons.commit(msgs)
    finally:
        cons.close()
    return {"replayed": moved, "unreplayable": skipped}
