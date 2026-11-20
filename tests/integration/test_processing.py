import itertools
import threading

import psycopg
import pytest

from paperforge.connectors.base import Page, SourceConnector, SourceRecord, WorkUnit
from paperforge.ingestion.runner import IngestionRunner
from paperforge.processing.batch import run_batch
from paperforge.processing.engine import PolarsEngine
from paperforge.processing.models import RawRecord
from paperforge.processing.normalizer import normalize_crossref
from paperforge.repositories.pg_paper_repository import PgPaperRepository
from paperforge.storage.local import LocalStorage
from tests.fixtures.papers import payload, ts

pytestmark = pytest.mark.integration


def rec(p, **kw):
    n = normalize_crossref(p)
    return RawRecord("crossref", n.doi, n.source_updated_at, p, **kw)


@pytest.fixture
def engine(pg_dsn):
    repo = PgPaperRepository(pg_dsn)
    yield PolarsEngine(repo)
    repo.close()


def q(dsn, sql, *a):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, a).fetchall()


def dump(dsn):
    """Comparable final state, independent of random paper_id / wall-clock columns."""
    papers = q(dsn, "SELECT primary_doi, title, content_hash, source_updated_at, cited_by_count, author_count, reference_count "
                    "FROM dwd.paper ORDER BY 1")
    # version_no is arrival-ordered by design, so compare the timeline (su, hash, valid_to) instead
    vers = q(dsn, "SELECT p.primary_doi, v.source_updated_at, v.content_hash, v.valid_to FROM dwd.paper_version v "
                  "JOIN dwd.paper p USING (paper_id) ORDER BY 1, 2, 3")
    auth = q(dsn, "SELECT p.primary_doi, a.ordinal, a.family FROM dwd.paper_author a JOIN dwd.paper p USING (paper_id) ORDER BY 1,2")
    refs = q(dsn, "SELECT p.primary_doi, count(*) FROM dwd.paper_reference r JOIN dwd.paper p USING (paper_id) GROUP BY 1 ORDER BY 1")
    return papers, vers, auth, refs


def test_insert_creates_paper_identifier_version_children(engine, pg_dsn):
    r = engine.process([rec(payload(1))])
    assert (r.inserted, r.updated, r.unchanged, r.quarantined) == (1, 0, 0, 0)
    ((pid, doi, ver, year, prec),) = q(pg_dsn, "SELECT paper_id, primary_doi, current_version, issued_year, issued_precision FROM dwd.paper")
    assert (doi, ver, year, prec) == ("10.99999/test.1", 1, 2020, "day")
    assert q(pg_dsn, "SELECT scheme, value, status, valid_to FROM dwd.paper_identifier") == [("doi", doi, "active", None)]
    assert q(pg_dsn, "SELECT family, orcid FROM dwd.paper_author") == [("Lovelace", "0000-0002-1825-0097")]
    assert q(pg_dsn, "SELECT ref_doi FROM dwd.paper_reference") == [("10.1/ref",)]
    assert q(pg_dsn, "SELECT version_no, valid_to, normalizer_version FROM dwd.paper_version") == [(1, None, 1)]


def test_duplicate_same_doi_no_new_entity_or_version(engine, pg_dsn):  # test #1
    for _ in range(3):
        engine.process([rec(payload(1)), rec(payload(1))])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(1,)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_identifier") == [(1,)]
    # DOI case / URL variants resolve to the same entity
    engine.process([rec(payload(1, DOI="https://doi.org/10.99999/TEST.1"))])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]


def test_metadata_change_creates_version_and_keeps_history(engine, pg_dsn):  # test #2 + historical query
    engine.process([rec(payload(1, indexed="2026-01-01T00:00:00Z", title="Old title"))])
    p2 = payload(1, indexed="2026-02-01T00:00:00Z", title="New title"); p2["author"].append({"given": "C", "family": "Babbage"})
    r = engine.process([rec(p2)])
    assert r.updated == 1
    assert q(pg_dsn, "SELECT title, current_version, author_count FROM dwd.paper") == [("New title", 2, 2)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_author") == [(2,)]
    v = q(pg_dsn, "SELECT version_no, snapshot->>'title', valid_from, valid_to FROM dwd.paper_version ORDER BY version_no")
    assert v[0][:2] == (1, "Old title") and v[0][3] == v[1][2] and v[1][3] is None   # v1 valid until v2 starts
    # "what did this paper look like on Jan 15?"
    assert q(pg_dsn, "SELECT snapshot->>'title' FROM dwd.paper_version WHERE valid_from <= %s AND (valid_to IS NULL OR valid_to > %s)",
             ts(15), ts(15)) == [("Old title",)]


def test_late_older_version_is_history_only(engine, pg_dsn):  # test #3
    engine.process([rec(payload(1, indexed="2026-03-01T00:00:00Z", title="v3"))])
    r = engine.process([rec(payload(1, indexed="2026-01-01T00:00:00Z", title="v1 arrives late"))])
    assert r.late == 1 and r.updated == 0
    assert q(pg_dsn, "SELECT title, current_version FROM dwd.paper") == [("v3", 1)]            # current untouched
    v = q(pg_dsn, "SELECT snapshot->>'title', source_updated_at, valid_to FROM dwd.paper_version ORDER BY source_updated_at")
    assert [x[0] for x in v] == ["v1 arrives late", "v3"] and v[0][2] == v[1][1] and v[1][2] is None


def test_out_of_order_arrival_converges_to_same_state(engine, pg_dsn, tmp_path):
    versions = [payload(1, indexed=f"2026-0{m}-01T00:00:00Z", title=f"t{m}") for m in (1, 2, 3)]
    expected = None
    for i, order in enumerate(itertools.permutations(versions)):
        dsn = pg_dsn
        with psycopg.connect(dsn, autocommit=True) as c:
            c.execute("TRUNCATE dwd.paper CASCADE")
        for p in order:                               # one micro-batch per event, in this arrival order
            engine.process([rec(p)])
        state = dump(dsn)
        assert state[0][0][1] == "t3"                  # the newest content always wins
        expected = expected or state
        assert state == expected, f"arrival order {i} diverged"


def test_batch_vs_single_event_processing_same_result(pg_dsn, engine):  # consistency, preview of #10
    recs = [rec(payload(n, indexed=f"2026-0{m}-01T00:00:00Z", title=f"p{n}v{m}")) for n in range(1, 6) for m in (1, 2)]
    engine.process(recs)                              # one big micro-batch
    big = dump(pg_dsn)
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute("TRUNCATE dwd.paper CASCADE")
    for r in recs:                                    # strictly one event at a time
        engine.process([r])
    assert dump(pg_dsn) == big


def test_volatile_changes_refresh_without_new_version(engine, pg_dsn):
    engine.process([rec(payload(1, indexed="2026-01-01T00:00:00Z"))])
    p = payload(1, indexed="2026-06-01T00:00:00Z"); p["is-referenced-by-count"] = 77; p["score"] = 5.0
    r = engine.process([rec(p)])
    assert r.refreshed == 1 and r.updated == 0
    assert q(pg_dsn, "SELECT cited_by_count, source_updated_at, current_version FROM dwd.paper") == [(77, ts(1, 6), 1)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(1,)]


def test_error_rule_quarantines_only_bad_records(engine, pg_dsn):  # test #8
    bad = payload(2, DOI="not-a-doi")
    r = engine.process([rec(payload(1)), rec(bad), rec(payload(3))])
    assert (r.received, r.inserted, r.quarantined) == (3, 2, 1)
    ((rid, ver, reason, status, pl),) = q(pg_dsn, "SELECT rule_id, rule_version, reason, status, payload FROM meta.quarantine_record")[0:1][0:1] or [None]
    assert (rid, ver, status) == ("doi_format", 1, "open") and pl["DOI"] == "not-a-doi" and "not-a-doi" in reason
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(2,)]


def test_warnings_keep_record_and_are_stored_with_version(engine, pg_dsn):
    p = payload(1, ISSN=["bad"], issued={"date-parts": [[2020, 13]]})
    p["title"] = []
    r = engine.process([rec(p)])
    assert (r.inserted, r.quarantined, r.warnings) == (1, 0, 3)
    flags = q(pg_dsn, "SELECT quality_flags FROM dwd.paper_version")[0][0]
    assert {f["rule_id"] for f in flags} == {"title_missing", "issn_invalid", "issued_invalid"}
    assert q(pg_dsn, "SELECT title, issued_date, issns FROM dwd.paper") == [(None, None, [])]


def test_missing_optional_fields_do_not_drop_paper(engine, pg_dsn):  # test #7
    p = payload(1)
    for k in ("author", "reference", "abstract", "member", "ISSN"):
        del p[k]
    p["surprise_field"] = [1, 2]
    assert engine.process([rec(p)]).inserted == 1
    assert q(pg_dsn, "SELECT author_count, abstract FROM dwd.paper") == [(0, None)]


def test_concurrent_writers_do_not_create_duplicate_entities(pg_dsn):
    barrier = threading.Barrier(4)
    errs = []

    def work():
        repo = PgPaperRepository(pg_dsn)
        eng = PolarsEngine(repo)
        try:
            barrier.wait()
            eng.process([rec(payload(n)) for n in range(1, 21)])
        except Exception as e:  # noqa: BLE001
            errs.append(e)
        finally:
            repo.close()

    ts_ = [threading.Thread(target=work) for _ in range(4)]
    [t.start() for t in ts_]; [t.join() for t in ts_]
    assert not errs, errs
    assert q(pg_dsn, "SELECT count(*), count(DISTINCT primary_doi) FROM dwd.paper") == [(20, 20)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(20,)]


class Fixtures(SourceConnector):
    name = "crossref"

    def plan(self, **_):
        return [WorkUnit("u")]

    def fetch(self, unit, cursor=None, on_request=None):
        recs = []
        for n in range(1, 31):
            p = payload(n)
            sr = normalize_crossref(p)
            recs.append(SourceRecord("crossref", sr.doi, sr.doi, sr.source_updated_at, p))
        recs.append(SourceRecord("crossref", "bad", None, None, {"title": ["no doi"]}))   # skipped at ingestion
        yield Page(recs[:15], "1")
        yield Page(recs[15:], None)


def test_batch_mode_from_raw_files_reconciles_with_ods(tmp_path, pg_dsn, engine):  # tests #9, #12 (batch path)
    storage = LocalStorage(tmp_path / "data")
    r = IngestionRunner(Fixtures(), storage, dsn=pg_dsn)
    r.run(r.start("bootstrap", Fixtures().plan(), {}, None))
    res = run_batch(engine, storage, pg_dsn, chunk_size=7)
    assert res.inserted == 30 and res.quarantined == 0
    assert q(pg_dsn, "SELECT count(*) FROM ods.crossref_works") == q(pg_dsn, "SELECT count(*) FROM dwd.paper")
    # every version traces back to an exact raw file + line whose DOI matches
    for doi, fid, line in q(pg_dsn, "SELECT p.primary_doi, v.raw_file_id, v.raw_line FROM dwd.paper_version v JOIN dwd.paper p USING (paper_id)"):
        import json
        path = q(pg_dsn, "SELECT path FROM ods.raw_file_manifest WHERE file_id=%s", fid)[0][0]
        assert json.loads(storage.read(path).splitlines()[line])["doi"] == doi
    # historical replay of the same raw files is idempotent
    res2 = run_batch(engine, storage, pg_dsn)
    assert (res2.inserted, res2.updated, res2.unchanged) == (0, 0, 30)
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(30,)]


def test_refresh_inside_same_chunk_equals_separate_chunks(pg_dsn, engine):
    """Regression: same content re-indexed later, arriving in the SAME micro-batch as the version that creates/updates the paper."""
    def versions():
        a = payload(1, indexed="2026-01-01T00:00:00Z", title="t"); a["is-referenced-by-count"] = 3
        b = payload(1, indexed="2026-06-01T00:00:00Z", title="t"); b["is-referenced-by-count"] = 50
        c = payload(1, indexed="2026-08-01T00:00:00Z", title="t2"); c["is-referenced-by-count"] = 60
        d = payload(1, indexed="2026-09-01T00:00:00Z", title="t2"); d["is-referenced-by-count"] = 70
        return [rec(a), rec(b), rec(c), rec(d)]
    engine.process(versions())                                    # all in one chunk
    one_chunk = dump(pg_dsn)
    assert q(pg_dsn, "SELECT cited_by_count, source_updated_at FROM dwd.paper") == [(70, ts(1, 9))]
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute("TRUNCATE dwd.paper CASCADE")
    for r in versions():                                          # one event at a time
        engine.process([r])
    assert dump(pg_dsn) == one_chunk
