import json

import duckdb
import polars as pl
import psycopg
import pytest

from paperforge.connectors.base import Page, SourceConnector
from paperforge.connectors.crossref_parquet import CrossrefParquetConnector
from paperforge.ingestion.runner import IngestionRunner
from paperforge.storage.local import LocalStorage

pytestmark = pytest.mark.integration

JOURNALS = {"computer": "Journal of Computer Science", "medicine": "Clinical Medicine", "life_science": "Cell Biology",
            "bioinformatics": "Bioinformatics", "other": "Journal of Cooking"}


def write_fixture(path, n_per_domain=10, ts="2026-01-01T00:00:00Z", tag="v1"):
    rows = []
    for d, j in JOURNALS.items():
        for i in range(n_per_domain):
            rows.append({"DOI": f"10.9999/{d}.{i}", "type": "journal-article", "title": [f"{tag} {d} {i}"],
                         "container-title": [j], "indexed": json.dumps({"date-time": ts}), "year": 2020})
    rows.append({"DOI": "10.9999/book", "type": "book", "title": ["b"], "container-title": ["x"], "indexed": "{}", "year": 1})
    df = pl.DataFrame(rows)  # noqa: F841 - read by DuckDB below
    # real Crossref parquet stores `indexed` as JSON type; mimic that
    duckdb.sql(f"COPY (SELECT * REPLACE (CAST(indexed AS JSON) AS indexed) FROM df) TO '{path}' (FORMAT parquet)")
    return 5 * n_per_domain


def runner(tmp_path, dsn, pq, cls=CrossrefParquetConnector):
    conn = cls("", files=[str(pq)], page_size=3)
    return IngestionRunner(conn, LocalStorage(tmp_path / "data"), dsn=dsn), conn


def q(dsn, sql, *a):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, a).fetchall()


def start_run(r, conn, limit):
    return r.run(r.start("bootstrap", conn.plan(limit=limit), {}, None))


def reconcile(tmp_path, dsn):
    """Raw files <-> manifest <-> ODS must agree (checksums, counts, lineage back to file+line)."""
    st = LocalStorage(tmp_path / "data")
    manifest = q(dsn, "SELECT file_id, path, record_count, sha256 FROM ods.raw_file_manifest")
    lines = 0
    for fid, path, cnt, sha in manifest:
        assert st.checksum(path) == sha
        got = [json.loads(x) for x in st.read(path).splitlines()]
        assert len(got) == cnt
        lines += cnt
        for ln, o in enumerate(got):  # every ODS row traces back to its exact raw line
            (doi,) = q(dsn, "SELECT doi_norm FROM ods.crossref_works WHERE raw_file_id=%s AND raw_line=%s", fid, ln)[0] if q(
                dsn, "SELECT 1 FROM ods.crossref_works WHERE raw_file_id=%s AND raw_line=%s", fid, ln) else (None,)
            assert doi in (None, o["doi"])
    assert set(st.list("raw")) == {m[1] for m in manifest}, "no unregistered/orphan raw files"
    return lines


def test_bootstrap_domains_quota_and_filters(tmp_path, pg_dsn):
    pq = tmp_path / "p.parquet"
    write_fixture(pq)
    r, conn = runner(tmp_path, pg_dsn, pq)
    res = start_run(r, conn, 20)
    assert res.status == "completed" and res.rows_seen == 20 == res.rows_inserted
    per = dict(q(pg_dsn, "SELECT payload->'container-title'->>0, count(*) FROM ods.crossref_works GROUP BY 1"))
    assert sum(per.values()) == 20 and "Journal of Cooking" in per  # 'other' quota (12%) -> at least 2
    assert not q(pg_dsn, "SELECT 1 FROM ods.crossref_works WHERE doi_norm='10.9999/book'")  # non journal-article excluded
    assert reconcile(tmp_path, pg_dsn) == 20


def test_duplicate_ingest_does_not_duplicate(tmp_path, pg_dsn):  # test #1
    pq = tmp_path / "p.parquet"
    write_fixture(pq)
    r, conn = runner(tmp_path, pg_dsn, pq)
    start_run(r, conn, 20)
    res2 = start_run(*[r, conn, 20])
    assert res2.rows_unchanged == 20 and res2.rows_inserted == 0 and res2.rows_updated == 0
    assert q(pg_dsn, "SELECT count(*), count(DISTINCT doi_norm) FROM ods.crossref_works") == [(20, 20)]
    assert reconcile(tmp_path, pg_dsn) == 40  # raw keeps both batches (immutable history)


def test_changed_and_late_records(tmp_path, pg_dsn):  # tests #2, #3 at ODS level
    pq = tmp_path / "p.parquet"
    write_fixture(pq, ts="2026-02-01T00:00:00Z", tag="v1")
    r, conn = runner(tmp_path, pg_dsn, pq)
    start_run(r, conn, 20)
    write_fixture(pq, ts="2026-03-01T00:00:00Z", tag="v2")  # newer + changed
    res = start_run(r, conn, 20)
    assert res.rows_updated == 20
    assert all(t.startswith("v2") for (t,) in q(pg_dsn, "SELECT payload->'title'->>0 FROM ods.crossref_works"))
    write_fixture(pq, ts="2026-01-01T00:00:00Z", tag="v0")  # older, arrives late -> must not overwrite
    res = start_run(r, conn, 20)
    assert res.rows_updated == 0 and res.rows_unchanged == 20
    assert all(t.startswith("v2") for (t,) in q(pg_dsn, "SELECT payload->'title'->>0 FROM ods.crossref_works"))
    assert reconcile(tmp_path, pg_dsn) == 60  # all three versions preserved in raw files


class Flaky(CrossrefParquetConnector):
    """Dies after `after` pages in the first life; behaves normally when `after` is None."""
    after = None

    def fetch(self, unit, cursor=None, on_request=None):
        n = 0
        for p in super().fetch(unit, cursor, on_request):
            if Flaky.after is not None and n >= Flaky.after:
                raise ConnectionError("simulated crash")
            n += 1
            yield p


def test_ingest_interrupted_and_resumed(tmp_path, pg_dsn):  # test #4
    pq = tmp_path / "p.parquet"
    write_fixture(pq, n_per_domain=40)
    r, conn = runner(tmp_path, pg_dsn, pq, Flaky)
    Flaky.after = 1
    bid = r.start("bootstrap", conn.plan(limit=100), {}, None)
    res = r.run(bid)
    assert res.status == "failed" and 0 < res.rows_seen < 100
    Flaky.after = None
    r2, conn2 = runner(tmp_path, pg_dsn, pq, Flaky)
    res = r2.run(r2.start("bootstrap", [], {}, None, batch_id=bid))
    assert res.status == "completed" and res.rows_seen == 100 and res.rows_inserted == 100
    assert q(pg_dsn, "SELECT count(*), count(DISTINCT doi_norm) FROM ods.crossref_works") == [(100, 100)]
    assert reconcile(tmp_path, pg_dsn) == 100


def test_orphan_file_from_crash_is_overwritten_not_duplicated(tmp_path, pg_dsn):
    pq = tmp_path / "p.parquet"
    write_fixture(pq, n_per_domain=10)
    r, conn = runner(tmp_path, pg_dsn, pq)
    bid = r.start("bootstrap", conn.plan(limit=20), {}, None)
    (bdate,) = q(pg_dsn, "SELECT params->>'date' FROM ods.ingestion_batch")[0]
    orphan = f"raw/source=crossref/date={bdate}/batch_id={bid}/part-000-0001.jsonl"
    LocalStorage(tmp_path / "data").write(orphan, b"garbage-from-crashed-run\n")
    res = r.run(bid)
    assert res.status == "completed"
    assert b"garbage" not in LocalStorage(tmp_path / "data").read(orphan)
    assert reconcile(tmp_path, pg_dsn) == 20


def test_completed_batch_cannot_be_reopened(tmp_path, pg_dsn):
    pq = tmp_path / "p.parquet"
    write_fixture(pq, n_per_domain=2)
    r, conn = runner(tmp_path, pg_dsn, pq)
    res = start_run(r, conn, 10)
    with pytest.raises(ValueError):
        r.start("bootstrap", [], {}, None, batch_id=res.batch_id)


def test_missing_doi_is_skipped_and_counted(tmp_path, pg_dsn):
    class NoDoi(SourceConnector):
        name = "crossref"

        def plan(self, **_):
            from paperforge.connectors.base import WorkUnit
            return [WorkUnit("u")]

        def fetch(self, unit, cursor=None, on_request=None):
            from paperforge.connectors.base import SourceRecord
            yield Page([SourceRecord("crossref", "", None, None, {"x": 1}),
                        SourceRecord("crossref", "10.1/a", "10.1/a", None, {"DOI": "10.1/a"})], None)

    r = IngestionRunner(NoDoi(), LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    res = r.run(r.start("bootstrap", NoDoi().plan(), {}, None))
    assert (res.rows_seen, res.rows_inserted, res.rows_skipped) == (2, 1, 1)


class TwoWindows(SourceConnector):
    name = "crossref"

    def plan(self, **_):
        from paperforge.connectors.base import WorkUnit
        return [WorkUnit("w1"), WorkUnit("w2")]

    def fetch(self, unit, cursor=None, on_request=None):
        from paperforge.connectors.base import SourceRecord
        recs = [SourceRecord("crossref", f"10.1/{unit.key}.{i}", f"10.1/{unit.key}.{i}", None, {"DOI": f"10.1/{unit.key}.{i}"}) for i in range(5)]
        yield Page(recs, None)


def test_limit_truncation_leaves_windows_pending(tmp_path, pg_dsn):
    """A window cut short by --limit must not be marked done (else the watermark would skip data)."""
    r = IngestionRunner(TwoWindows(), LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    res = r.run(r.start("incremental", TwoWindows().plan(), {}, 7))
    assert res.truncated and res.status == "completed" and res.rows_seen == 7
    st = dict(q(pg_dsn, "SELECT unit_key, status FROM meta.sync_checkpoint"))
    assert st == {"w1": "done", "w2": "pending"} or st == {"w1": "pending", "w2": "done"}
    # resuming without a limit finishes the remaining window; nothing duplicated
    r2 = IngestionRunner(TwoWindows(), LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute("UPDATE ods.ingestion_batch SET params = params - 'limit'")
    res2 = r2.run(r2.start("incremental", [], {}, None, batch_id=res.batch_id))
    assert not res2.truncated and res2.status == "completed"
    assert q(pg_dsn, "SELECT count(*) FROM ods.crossref_works") == [(10,)]


# ---- targeted ingestion by DOI --------------------------------------------------------------
def _api(handler):
    import httpx

    from paperforge.connectors.crossref_api import CrossrefApiConnector
    return CrossrefApiConnector("http://x/works", rate_per_sec=0, backoff_initial=0.001, transport=httpx.MockTransport(handler))


def _crossref_handler(req):
    import httpx
    d = req.url.path.removeprefix("/works/")
    if d == "10.1/missing":
        return httpx.Response(404, text="Resource not found.")
    return httpx.Response(200, json={"message": {"DOI": d, "type": "journal-article", "title": ["T " + d],
                                                 "indexed": {"date-time": "2026-05-01T00:00:00Z"}}})


def test_targeted_ingest_one_bad_doi_does_not_block_others_and_keeps_watermark(tmp_path, pg_dsn):
    conn = _api(_crossref_handler)
    r = IngestionRunner(conn, LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    res = r.run(r.start("targeted", conn.plan_dois(["10.1/a", "10.1/missing", "10.1/b"]), {}, None))
    assert res.status == "failed" and res.units_done == 2 and res.units_failed == 1 and res.rows_inserted == 2
    assert sorted(x[0] for x in q(pg_dsn, "SELECT doi_norm FROM ods.crossref_works")) == ["10.1/a", "10.1/b"]
    assert q(pg_dsn, "SELECT mode FROM ods.ingestion_batch") == [("targeted",)]
    # raw lineage exists, and a targeted fetch never writes a sync watermark
    assert q(pg_dsn, "SELECT count(*) FROM ods.crossref_works w JOIN ods.raw_file_manifest m ON m.file_id=w.raw_file_id") == [(2,)]
    assert q(pg_dsn, "SELECT count(*) FROM meta.sync_checkpoint WHERE unit_key='watermark'") == [(0,)]
    # fetching an unchanged record again is idempotent
    r2 = IngestionRunner(conn, LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    res2 = r2.run(r2.start("targeted", conn.plan_dois(["10.1/a"]), {}, None))
    assert res2.rows_unchanged == 1 and res2.rows_inserted == 0


def test_outbox_can_be_limited_to_given_dois(tmp_path, pg_dsn):
    from paperforge.events.bus import EventBus
    from paperforge.events.publisher import publish_outbox

    class Rec(EventBus):
        def __init__(self):
            self.keys = []

        def publish(self, items):
            self.keys += [i.key.decode() for i in items]
            return [None] * len(items)

        def consumer(self, *a, **k): ...
        def ensure_topics(self, *a, **k): ...
        def lag(self, *a, **k): ...

    conn = _api(_crossref_handler)
    r = IngestionRunner(conn, LocalStorage(tmp_path / "data"), dsn=pg_dsn)
    r.run(r.start("targeted", conn.plan_dois(["10.1/a", "10.1/b"]), {}, None))
    bus = Rec()
    assert publish_outbox(bus, "t", dsn=pg_dsn, dois=["10.1/a"])["published"] == 1
    assert bus.keys == ["10.1/a"]
    assert q(pg_dsn, "SELECT doi_norm FROM ods.crossref_works WHERE published_at IS NULL") == [("10.1/b",)]
    assert publish_outbox(bus, "t", dsn=pg_dsn, dois=["10.1/a"])["published"] == 0   # already announced
