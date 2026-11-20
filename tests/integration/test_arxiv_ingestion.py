import json

import psycopg
import pytest

from paperforge.connectors.arxiv_snapshot import ArxivSnapshotConnector
from paperforge.ingestion.runner import IngestionRunner
from paperforge.processing.batch import run_batch
from paperforge.processing.engine import PolarsEngine
from paperforge.processing.models import RawRecord
from paperforge.repositories.pg_paper_repository import PgPaperRepository
from paperforge.storage.local import LocalStorage
from tests.fixtures.papers import payload
from tests.unit.test_arxiv import rec as arx_payload

pytestmark = pytest.mark.integration


def ar(**kw):
    p = arx_payload(**kw)
    return RawRecord("arxiv", p.get("doi"), None, p, source_id=p["id"])


def cr(n=1, **kw):
    p = payload(n, **kw)
    return RawRecord("crossref", p["DOI"].lower(), None, p)


@pytest.fixture
def engine(pg_dsn):
    repo = PgPaperRepository(pg_dsn)
    yield PolarsEngine(repo)
    repo.close()


def q(dsn, sql, *a):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, a).fetchall()


DOI = "10.99999/test.1"            # what payload(1) normalizes to


def test_arxiv_only_paper_is_a_first_class_paper_keyed_arxiv_id(engine, pg_dsn):
    r = engine.process([ar()])
    assert (r.inserted, r.quarantined) == (1, 0)
    assert q(pg_dsn, "SELECT paper_key, primary_doi, type, issued_year, author_count FROM dwd.paper") == [("arxiv:2101.00001", None, "preprint", 2021, 3)]
    assert q(pg_dsn, "SELECT scheme, value, source FROM dwd.paper_identifier") == [("arxiv", "2101.00001", "arxiv")]
    assert q(pg_dsn, "SELECT arxiv_id, primary_category, categories, version_count FROM dwd.paper_arxiv") == [("2101.00001", "cs.LG", ["cs.LG", "stat.ML"], 2)]
    assert q(pg_dsn, "SELECT version_no, source FROM dwd.paper_version") == [(1, "arxiv")]


def test_doi_match_with_crossref_paper_adds_identity_and_never_a_version(engine, pg_dsn):
    engine.process([cr(1)])
    (before,) = q(pg_dsn, "SELECT paper_id, title, publisher, content_hash, current_version FROM dwd.paper")
    r = engine.process([ar(doi=DOI.upper())])
    assert (r.inserted, r.updated, r.unchanged) == (1, 0, 0)                      # counted as one arXiv row written
    assert q(pg_dsn, "SELECT paper_id, title, publisher, content_hash, current_version FROM dwd.paper") == [before]   # untouched
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(1,)]
    assert q(pg_dsn, "SELECT a.arxiv_id, a.paper_id = %s FROM dwd.paper_arxiv a", before[0]) == [("2101.00001", True)]
    assert sorted(q(pg_dsn, "SELECT scheme, value, source FROM dwd.paper_identifier")) == [("arxiv", "2101.00001", "arxiv"), ("doi", DOI, "crossref")]
    # an arXiv update (new category set, new abstract) refreshes paper_arxiv only
    r = engine.process([ar(doi=DOI, categories="cs.LG cs.AI", abstract="changed")])
    assert (r.updated, r.inserted) == (1, 0)
    assert q(pg_dsn, "SELECT categories, abstract FROM dwd.paper_arxiv") == [(["cs.LG", "cs.AI"], "changed")]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(1,)]
    assert engine.process([ar(doi=DOI, categories="cs.LG cs.AI", abstract="changed")]).unchanged == 1


def test_arxiv_first_then_crossref_is_one_paper_and_crossref_becomes_current(engine, pg_dsn):
    engine.process([ar(doi=DOI)])
    assert q(pg_dsn, "SELECT paper_key, primary_doi FROM dwd.paper") == [(DOI, DOI)]
    engine.process([cr(1)])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]
    assert q(pg_dsn, "SELECT title, publisher, current_version FROM dwd.paper") == [("Paper 1", "Test Press", 2)]
    assert q(pg_dsn, "SELECT source, valid_to IS NULL FROM dwd.paper_version ORDER BY source_updated_at") == [("arxiv", False), ("crossref", True)]
    # now Crossref owns the content: a changed arXiv record must not create a version
    engine.process([ar(doi=DOI, title="Different title")])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(2,)]
    assert q(pg_dsn, "SELECT title FROM dwd.paper") == [("Paper 1",)]
    assert q(pg_dsn, "SELECT title FROM dwd.paper_arxiv") == [("Different title",)]


@pytest.mark.parametrize("same_chunk", [True, False])
def test_two_arxiv_ids_for_one_doi_are_one_paper(engine, pg_dsn, same_chunk):
    a, b = ar(i="2101.00001", doi=DOI), ar(i="2102.00002", doi=DOI, title="Second submission")
    if same_chunk:
        engine.process([a, b])
    else:
        engine.process([a]); engine.process([b])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(1,)]               # no flip-flop between the two ids
    assert q(pg_dsn, "SELECT arxiv_id FROM dwd.paper_arxiv ORDER BY 1") == [("2101.00001",), ("2102.00002",)]
    assert q(pg_dsn, "SELECT count(DISTINCT paper_id) FROM dwd.paper_arxiv") == [(1,)]
    assert q(pg_dsn, "SELECT title FROM dwd.paper") == [("A study of $a<b$ and $c>d$",)]


def test_arxiv_only_paper_updates_by_version_until_it_gains_a_doi(engine, pg_dsn):
    engine.process([ar()])
    r = engine.process([ar(title="Better title", update_date="2021-06-01")])
    assert r.updated == 1
    assert q(pg_dsn, "SELECT title, current_version FROM dwd.paper") == [("Better title", 2)]
    # the later snapshot knows the DOI: it is recorded on the same paper (key stays arxiv:<id>)
    engine.process([ar(title="Better title", update_date="2021-06-01", doi=DOI)])
    assert q(pg_dsn, "SELECT count(*), max(paper_key), max(primary_doi) FROM dwd.paper") == [(1, "arxiv:2101.00001", DOI)]
    assert sorted(q(pg_dsn, "SELECT scheme, value FROM dwd.paper_identifier")) == [("arxiv", "2101.00001"), ("doi", DOI)]
    # ... and Crossref's record for that DOI lands on the same paper instead of creating a duplicate
    engine.process([cr(1)])
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(1,)]
    # arXiv v1, arXiv v2 (title), arXiv v3 (gained the DOI: part of the content), Crossref
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(4,)]


def test_invalid_arxiv_record_is_quarantined_by_arxiv_id(engine, pg_dsn):
    r = engine.process([ar(i="not an id")])
    assert (r.quarantined, r.inserted) == (1, 0)
    assert q(pg_dsn, "SELECT source, source_id, rule_id FROM meta.quarantine_record") == [("arxiv", "not an id", "arxiv_id_invalid")]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper") == [(0,)]


def test_snapshot_end_to_end_ods_raw_dwd_and_idempotent_rerun(pg_dsn, tmp_path):
    lines = [arx_payload(f"2101.{i:05d}", categories="cs.LG" if i <= 6 else "math.ST", doi=(DOI if i == 1 else None)) for i in range(1, 10)]
    f = tmp_path / "snap.json"
    f.write_text("\n".join(json.dumps(x) for x in lines) + "\n")
    storage = LocalStorage(str(tmp_path / "data"))
    conn = ArxivSnapshotConnector(str(f), ["cs.*"], page_size=4)
    runner = IngestionRunner(conn, storage, dsn=pg_dsn)
    res = runner.run(runner.start("bootstrap", conn.plan(), {"file": "snap.json"}, None))
    assert (res.status, res.rows_seen, res.rows_inserted) == ("completed", 6, 6)
    assert q(pg_dsn, "SELECT count(*) FROM ods.arxiv_works") == [(6,)]
    assert q(pg_dsn, "SELECT count(*) FROM ods.crossref_works") == [(0,)]
    assert q(pg_dsn, "SELECT count(*) FROM ods.arxiv_works w JOIN ods.raw_file_manifest m ON m.file_id = w.raw_file_id") == [(6,)]
    repo = PgPaperRepository(pg_dsn)
    try:
        t = run_batch(PolarsEngine(repo), storage, dsn=pg_dsn)
    finally:
        repo.close()
    assert (t.inserted, t.quarantined) == (6, 0)
    assert q(pg_dsn, "SELECT count(*), count(primary_doi) FROM dwd.paper") == [(6, 1)]
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_arxiv a JOIN ods.arxiv_works w USING (arxiv_id)") == [(6,)]
    # second ingestion of the same snapshot: no ODS change, replaying every raw file changes nothing in DWD
    runner2 = IngestionRunner(conn, storage, dsn=pg_dsn)
    res2 = runner2.run(runner2.start("bootstrap", conn.plan(), {"file": "snap.json"}, None))
    assert (res2.rows_inserted, res2.rows_updated, res2.rows_unchanged) == (0, 0, 6)
    repo = PgPaperRepository(pg_dsn)
    try:
        t2 = run_batch(PolarsEngine(repo), storage, dsn=pg_dsn)
    finally:
        repo.close()
    assert (t2.inserted, t2.updated) == (0, 0) and t2.unchanged == 12
    assert q(pg_dsn, "SELECT count(*) FROM dwd.paper_version") == [(6,)]
