import json
import time
import uuid

import psycopg
import pytest

from paperforge.connectors.base import Page, SourceConnector, SourceRecord, WorkUnit
from paperforge.events.bus import Outgoing
from paperforge.events.kafka_bus import KafkaEventBus
from paperforge.events.publisher import publish_outbox, publish_raw_files
from paperforge.events.schema import Event, make_event
from paperforge.ingestion.runner import IngestionRunner
from paperforge.processing.batch import run_batch
from paperforge.processing.engine import PolarsEngine
from paperforge.processing.normalizer import normalize_crossref, raw_payload_hash
from paperforge.repositories.pg_paper_repository import PgPaperRepository
from paperforge.storage.local import LocalStorage
from paperforge.workers.consumer import StreamWorker, replay_dlq
from tests.fixtures.papers import payload
from tests.integration.test_processing import dump, q

pytestmark = pytest.mark.integration
BOOT = None


@pytest.fixture(scope="module")
def bus():
    from paperforge.config.settings import get_settings
    return KafkaEventBus(get_settings().kafka_bootstrap)


@pytest.fixture
def topics(bus):
    raw, dlq = f"t.raw.{uuid.uuid4().hex[:8]}", f"t.dlq.{uuid.uuid4().hex[:8]}"
    bus.ensure_topics({raw: 3, dlq: 1})
    return raw, dlq


@pytest.fixture
def repo(pg_dsn):
    r = PgPaperRepository(pg_dsn)
    yield r
    r.close()


def P(n, **kw):
    kw.setdefault("DOI", f"10.99999/stream.{n}")
    return payload(n, **kw)


def ev(p, trace="t"):
    n = normalize_crossref(p)
    return make_event("crossref", n.doi, p, raw_payload_hash(p), n.source_updated_at, trace)


def send(bus, topic, events, headers=None):
    items = [Outgoing(topic, e.key(), e.to_bytes(), headers or {}) for e in events]
    assert all(r is None for r in bus.publish(items))


def mk_worker(bus, repo, topics, group, batch=20, engine=None, **kw):
    raw, dlq = topics
    return StreamWorker(bus, engine or PolarsEngine(repo), group, raw, dlq, batch_size=batch, batch_timeout_s=0.5,
                        retry_backoff_s=0.01, on_reconnect=repo.reconnect, **kw)


def run_until(w, n_messages, timeout=60):
    t0 = time.monotonic()
    while w.stats.messages < n_messages and time.monotonic() - t0 < timeout:
        w.step()
    return w.stats


def read_all(bus, topic, n, timeout=30):
    c = bus.consumer(f"reader-{uuid.uuid4().hex[:6]}", [topic])
    out, t0 = [], time.monotonic()
    while len(out) < n and time.monotonic() - t0 < timeout:
        out += c.poll_batch(100, 0.5)
    c.close()
    return out


# ------------------------------------------------------------------------------------------ event contract
def test_event_contract_key_and_determinism():
    p = P(1)
    e1, e2 = ev(p), ev(p)
    assert e1.event_id == e2.event_id                       # same content -> same id
    assert ev(P(1, title="changed")).event_id != e1.event_id
    assert e1.key() == b"10.99999/stream.1" and e1.schema_version == 1 and e1.event_type == "paper.upserted"
    assert set(e1.model_dump()) == {"event_id", "event_type", "schema_version", "source", "source_id", "doi", "source_updated_at",
                                    "ingested_at", "trace_id", "payload_hash", "payload"}
    with pytest.raises(Exception):
        Event.model_validate({**e1.model_dump(mode="json"), "surprise": 1})


def test_same_doi_always_same_partition_and_publish_acks(bus, topics):
    raw, _ = topics
    evs = [ev(P(n, indexed=f"2026-0{v}-01T00:00:00Z", title=f"n{n}v{v}")) for n in range(1, 8) for v in (1, 2, 3)]
    send(bus, raw, evs)
    msgs = read_all(bus, raw, len(evs))
    assert len(msgs) == len(evs)
    parts = {}
    for m in msgs:
        parts.setdefault(m.key, set()).add(m.partition)
    assert all(len(v) == 1 for v in parts.values()) and len({p for v in parts.values() for p in v}) > 1   # spread, but per-DOI stable


# ------------------------------------------------------------------------------------------ streaming path
def test_streaming_creates_papers_and_commits_after_persist(bus, topics, repo, pg_dsn):
    raw, _ = topics
    send(bus, raw, [ev(P(n)) for n in range(1, 31)], {"raw_file_id": b"7", "raw_line": b"3"})
    w = mk_worker(bus, repo, topics, "g-basic")
    st = run_until(w, 30)
    w.consumer.close()
    assert (st.result.inserted, st.dlq) == (30, 0)
    assert q(pg_dsn, "SELECT count(*), count(DISTINCT primary_doi) FROM dwd.paper") == [(30, 30)]
    assert q(pg_dsn, "SELECT DISTINCT raw_file_id, raw_line FROM dwd.paper_version") == [(7, 3)]      # lineage via headers
    lag = bus.lag("g-basic", raw)
    assert sum(end - max(c, 0) for c, end in lag.values()) == 0


def test_consumer_interrupted_then_resumes_from_committed_offset(bus, topics, repo, pg_dsn):  # test #5
    raw, _ = topics
    send(bus, raw, [ev(P(n)) for n in range(1, 101)])
    w1 = mk_worker(bus, repo, topics, "g-resume", batch=20)
    for _ in range(40):                                    # run until >=2 batches done, then "kill" it
        w1.step()
        if w1.stats.batches >= 2:
            break
    done = w1.stats.messages
    w1.consumer.close()
    assert 0 < done < 100
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(done,)]
    lag = bus.lag("g-resume", raw)
    assert sum(end - max(c, 0) for c, end in lag.values()) == 100 - done      # progress was committed, remainder pending
    w2 = mk_worker(bus, repo, topics, "g-resume", batch=20)                    # restart, same consumer group
    run_until(w2, 100 - done)
    w2.consumer.close()
    assert w2.stats.messages == 100 - done and w2.stats.result.inserted == 100 - done   # nothing re-processed
    assert q(pg_dsn, "SELECT count(*), count(DISTINCT primary_doi) FROM dwd.paper") == [(100, 100)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(100,)]


class Crash(RuntimeError):
    pass


def test_write_succeeded_but_offset_not_committed(bus, topics, repo, pg_dsn):  # test #6
    raw, _ = topics
    send(bus, raw, [ev(P(n)) for n in range(1, 21)])

    def die(_msgs):
        raise Crash("killed after DB commit, before offset commit")

    w1 = mk_worker(bus, repo, topics, "g-crash", batch=20, before_commit=die)
    with pytest.raises(Crash):
        for _ in range(30):
            w1.step()
    w1.consumer.close()
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(20,)]              # data IS durable
    lag = bus.lag("g-crash", raw)
    assert sum(end - max(c, 0) for c, end in lag.values()) == 20               # ... but progress was NOT committed
    w2 = mk_worker(bus, repo, topics, "g-crash", batch=20)
    run_until(w2, 20)
    w2.consumer.close()
    r = w2.stats.result
    assert (r.received, r.unchanged, r.inserted, r.updated) == (20, 20, 0, 0)    # redelivery is a pure no-op
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(20,)] and q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(20,)]
    assert sum(end - max(c, 0) for c, end in bus.lag("g-crash", raw).values()) == 0


class PoisonEngine(PolarsEngine):
    poison = "10.99999/stream.13"

    def process(self, records, quarantine=True):
        if any(r.doi == self.poison for r in records):
            raise ValueError("boom: cannot process this event")
        return super().process(records, quarantine)


def test_poison_events_go_to_dlq_and_can_be_replayed(bus, topics, repo, pg_dsn):
    raw, dlq = topics
    good = [ev(P(n)) for n in range(11, 16)]                                     # includes the poison DOI stream.13
    send(bus, raw, good)
    bad_json = Outgoing(raw, b"k1", b"{not json", {})
    missing = Outgoing(raw, b"k2", json.dumps({"event_id": "x"}).encode(), {})
    future = ev(P(20)).model_copy(update={"schema_version": 99})
    assert all(r is None for r in bus.publish([bad_json, missing, Outgoing(raw, b"k3", future.to_bytes(), {})]))
    invalid_doi = ev(P(21, DOI="not-a-doi"))                                     # validation failure != processing failure
    send(bus, raw, [invalid_doi])
    w = mk_worker(bus, repo, topics, "g-dlq", batch=50, engine=PoisonEngine(repo))
    run_until(w, 9)
    w.consumer.close()
    assert w.stats.dlq == 4                                                      # bad json, missing fields, future schema, poison
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(4,)]                 # the 4 healthy ones were NOT blocked by the poison
    assert q(pg_dsn, "SELECT rule_id, status FROM meta.quarantine_record") == [("doi_format", "open")]   # quarantine, not DLQ
    assert sum(end - max(c, 0) for c, end in bus.lag("g-dlq", raw).values()) == 0   # poison did not stall the partition
    dl = [json.loads(m.value) for m in read_all(bus, dlq, 4)]
    kinds = sorted(d["error_type"] for d in dl)
    assert kinds == ["ValueError", "invalid_event", "invalid_event", "invalid_event"]
    pz = next(d for d in dl if d["error_type"] == "ValueError")
    assert "boom" in pz["error"] and pz["event"]["doi"] == PoisonEngine.poison and pz["origin"]["topic"] == raw and pz["attempts"] >= 1
    # fix the cause, replay the DLQ, process again
    out = replay_dlq(bus, dlq, raw, "g-dlq-replay", idle_exit_s=3)
    assert out == {"replayed": 1, "unreplayable": 3}
    w2 = mk_worker(bus, repo, topics, "g-dlq")                                   # healthy engine now
    run_until(w2, 1)
    w2.consumer.close()
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper WHERE primary_doi = %s", PoisonEngine.poison) == [(1,)]


class FlakyDB(PolarsEngine):
    def __init__(self, repo, fail_times):
        super().__init__(repo)
        self.left, self.calls = fail_times, 0

    def process(self, records, quarantine=True):
        self.calls += 1
        if self.left > 0:
            self.left -= 1
            raise psycopg.OperationalError("connection lost")
        return super().process(records, quarantine)


def test_transient_db_errors_are_retried_not_dead_lettered(bus, topics, repo, pg_dsn):
    raw, _ = topics
    send(bus, raw, [ev(P(n)) for n in range(1, 6)])
    eng = FlakyDB(repo, fail_times=2)
    w = mk_worker(bus, repo, topics, "g-flaky", engine=eng)
    run_until(w, 5)
    w.consumer.close()
    assert w.stats.dlq == 0 and eng.calls == 3 and q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(5,)]


def test_persistent_db_outage_crashes_worker_without_committing(bus, topics, repo, pg_dsn):
    raw, dlq = topics
    send(bus, raw, [ev(P(n)) for n in range(1, 4)])
    w = mk_worker(bus, repo, topics, "g-outage", engine=FlakyDB(repo, fail_times=99))
    with pytest.raises(psycopg.OperationalError):
        for _ in range(20):
            w.step()
    w.consumer.close()
    assert sum(end - max(c, 0) for c, end in bus.lag("g-outage", raw).values()) == 3    # nothing committed, nothing dead-lettered
    assert read_all(bus, dlq, 1, timeout=2) == []


def test_redelivered_invalid_event_is_quarantined_once(bus, topics, repo, pg_dsn):
    raw, _ = topics
    e = ev(P(1, DOI="bad doi"))
    send(bus, raw, [e, e, e])
    w = mk_worker(bus, repo, topics, "g-qdup")
    run_until(w, 3)
    w.consumer.close()
    assert q(pg_dsn, "SELECT count(*) FROM meta.quarantine_record WHERE status='open'") == [(1,)]


# ------------------------------------------------------------------------------------------ outbox
class FakeBus(KafkaEventBus):
    """Does not touch Kafka. Lets a test decide which sends fail and run code between send and ack."""

    def __init__(self, fail_keys=(), during=None):
        self.fail_keys, self.during, self.sent = set(fail_keys), during, []

    def publish(self, items):
        self.sent += items
        if self.during:
            self.during()
        return [RuntimeError("nack") if it.key.decode() in self.fail_keys else None for it in items]


def seed_ods(dsn, n):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("INSERT INTO ods.ingestion_batch(batch_id,source,mode,status,params) VALUES ('o','crossref','bootstrap','completed','{}')")
        fid = c.execute("INSERT INTO ods.raw_file_manifest(batch_id,unit_key,seq,path,record_count,size_bytes,sha256) VALUES ('o','u',1,'o',1,1,'x') RETURNING file_id").fetchone()[0]
        for i in range(1, n + 1):
            p = P(i); nn = normalize_crossref(p)
            c.execute("INSERT INTO ods.crossref_works(doi_norm,source,source_id,payload,payload_hash,source_updated_at,batch_id,raw_file_id,raw_line) "
                      "VALUES (%s,'crossref',%s,%s,%s,%s,'o',%s,%s)", (nn.doi, nn.doi, json.dumps(p), raw_payload_hash(p), nn.source_updated_at, fid, i))


def test_outbox_marks_published_only_after_ack_and_only_for_sent_version(pg_dsn):
    seed_ods(pg_dsn, 4)
    fb = FakeBus(fail_keys={"10.99999/stream.2"})
    res = publish_outbox(fb, "t", pg_dsn)
    assert res["published"] == 3 and res["failed"] == 1
    assert q(pg_dsn, "SELECT doi_norm FROM ods.crossref_works WHERE published_at IS NULL") == [("10.99999/stream.2",)]
    # a row updated between send and ack must stay pending (its new content was never announced)
    def mutate():
        with psycopg.connect(pg_dsn, autocommit=True) as c:
            c.execute("UPDATE ods.crossref_works SET payload_hash='newer', published_at=NULL WHERE doi_norm='10.99999/stream.2'")
    res = publish_outbox(FakeBus(during=mutate), "t", pg_dsn, limit=1)   # a single pass; the next pass would legitimately re-announce it
    assert q(pg_dsn, "SELECT payload_hash, published_at IS NULL FROM ods.crossref_works WHERE doi_norm='10.99999/stream.2'") == [("newer", True)]
    assert q(pg_dsn, "SELECT count(*) FROM ods.crossref_works WHERE published_at IS NULL") == [(1,)]


# ------------------------------------------------------------------------------------------ replay & consistency
class History(SourceConnector):
    """Raw history with several versions per paper, out of order, a duplicate and an invalid record."""
    name = "crossref"
    PAGES = [
        [P(1, indexed="2026-02-01T00:00:00Z", title="A v2"), P(2, indexed="2026-01-01T00:00:00Z", title="B v1")],
        [P(1, indexed="2026-01-01T00:00:00Z", title="A v1"), P(2, indexed="2026-03-01T00:00:00Z", title="B v3")],   # A v1 arrives LATE
        [P(1, indexed="2026-02-01T00:00:00Z", title="A v2"), P(3, DOI="10.99999/stream.bad space"),                  # duplicate + invalid
         P(4, indexed="2026-01-01T00:00:00Z"), P(4, indexed="2026-05-01T00:00:00Z")],                                # volatile-only change
    ]

    def plan(self, **_):
        return [WorkUnit("u")]

    def fetch(self, unit, cursor=None, on_request=None):
        for i, page in enumerate(self.PAGES):
            recs = []
            for p in page:
                n = normalize_crossref(p)
                recs.append(SourceRecord("crossref", n.doi, n.doi, n.source_updated_at, p))
            yield Page(recs, None if i == len(self.PAGES) - 1 else str(i + 1))


def ingest_history(tmp_path, dsn):
    storage = LocalStorage(tmp_path / "data")
    r = IngestionRunner(History(), storage, dsn=dsn)
    r.run(r.start("bootstrap", History().plan(), {}, None))
    return storage


def wipe_dwd(dsn):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("TRUNCATE dwd.paper, meta.quarantine_record CASCADE")


def quarantine_state(dsn):
    return q(dsn, "SELECT source_id, rule_id, status FROM meta.quarantine_record ORDER BY 1, 2")


def test_batch_and_streaming_produce_identical_results(bus, topics, repo, pg_dsn, tmp_path):  # test #10
    storage = ingest_history(tmp_path, pg_dsn)
    run_batch(PolarsEngine(repo), storage, pg_dsn, chunk_size=3)
    batch_state, batch_q = dump(pg_dsn), quarantine_state(pg_dsn)
    assert len(batch_state[0]) == 3 and batch_q                                   # 3 papers (A, B, 4); one quarantined record
    versions = {r[0]: r for r in batch_state[1]}
    assert sum(1 for v in batch_state[1] if v[0].endswith("stream.1")) == 2       # A: v1 + v2 (duplicate collapsed)
    assert sum(1 for v in batch_state[1] if v[0].endswith("stream.4")) == 1       # volatile-only change: still one version

    wipe_dwd(pg_dsn)
    raw, _ = topics
    res = publish_raw_files(bus, raw, storage, pg_dsn)
    assert res["failed"] == 0
    w = mk_worker(bus, repo, topics, "g-consistency", batch=4)
    run_until(w, res["published"])
    w.consumer.close()
    assert dump(pg_dsn) == batch_state and quarantine_state(pg_dsn) == batch_q


def test_historical_replay_rebuilds_same_state_and_is_idempotent(bus, topics, repo, pg_dsn, tmp_path):  # test #9
    storage = ingest_history(tmp_path, pg_dsn)
    raw, _ = topics
    n = publish_raw_files(bus, raw, storage, pg_dsn)["published"]
    w = mk_worker(bus, repo, topics, "g-replay-1")
    run_until(w, n); w.consumer.close()
    first = dump(pg_dsn)
    # (a) replay everything again with a NEW group: pure no-op
    w = mk_worker(bus, repo, topics, "g-replay-2")
    run_until(w, n); w.consumer.close()
    for name, a, b in zip(("papers", "versions", "authors", "refs"), dump(pg_dsn), first):
        assert a == b, f"{name} changed by an idempotent replay"
    assert w.stats.result.inserted == 0 and w.stats.result.updated == 0
    # (b) lose the warehouse, replay history again with another group: identical rebuild
    wipe_dwd(pg_dsn)
    w = mk_worker(bus, repo, topics, "g-replay-3")
    run_until(w, n); w.consumer.close()
    assert dump(pg_dsn) == first
    # (c) a single batch can be replayed on its own
    bid = q(pg_dsn, "SELECT batch_id FROM ods.ingestion_batch")[0][0]
    assert publish_raw_files(bus, raw, storage, pg_dsn, batch_id=bid)["published"] == n
