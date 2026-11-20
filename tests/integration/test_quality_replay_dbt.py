import json
import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from paperforge.processing.engine import PolarsEngine
from paperforge.processing.models import RawRecord
from paperforge.processing.normalizer import normalize_crossref
from paperforge.quality.replay import replay_quarantine
from paperforge.quality.report import build_report
from paperforge.quality.runner import has_blocking_failure, run_all
from paperforge.repositories.pg_paper_repository import PgPaperRepository
from tests.fixtures.papers import payload

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


def rec(p, fid=None):
    n = normalize_crossref(p)
    return RawRecord("crossref", n.doi, n.source_updated_at, p, None, fid, 0 if fid else None)


def load(engine, dsn, payloads) -> object:
    """Mimic the real pipeline: record in ODS (with lineage to a registered raw file) and then process it."""
    with psycopg.connect(dsn, autocommit=True) as c:
        row = c.execute("SELECT file_id FROM ods.raw_file_manifest WHERE path='fx.jsonl'").fetchone()
    fid = row[0] if row else make_raw_file(dsn)
    with psycopg.connect(dsn, autocommit=True) as c:
        for pl in payloads:
            n = normalize_crossref(pl)
            c.execute("INSERT INTO ods.crossref_works(doi_norm,source,source_id,payload,payload_hash,source_updated_at,batch_id,raw_file_id,raw_line) "
                      "VALUES (%s,'crossref',%s,%s,'h',%s,'fx',%s,0) ON CONFLICT (doi_norm) DO UPDATE SET payload=EXCLUDED.payload",
                      (n.doi, n.doi, json.dumps(pl), n.source_updated_at, fid))
    return engine.process([rec(pl, fid) for pl in payloads])


def make_raw_file(dsn) -> int:
    """A registered raw file, so that versions have valid lineage (the lineage rule is a real Error rule)."""
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("INSERT INTO ods.ingestion_batch(batch_id,source,mode,status,params) VALUES ('fx','crossref','bootstrap','completed','{}')")
        return c.execute("INSERT INTO ods.raw_file_manifest(batch_id,unit_key,seq,path,record_count,size_bytes,sha256) "
                         "VALUES ('fx','u',1,'fx.jsonl',1,1,'x') RETURNING file_id").fetchone()[0]


def P(n, **kw):
    kw.setdefault("DOI", f"10.99999/dbt.{n}")
    return payload(n, **kw)


@pytest.fixture
def engine(pg_dsn):
    repo = PgPaperRepository(pg_dsn)
    yield PolarsEngine(repo)
    repo.close()


def q(dsn, sql, *a):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, a or None).fetchall()


def ex(dsn, sql, *a):
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(sql, a or None)


# ---------------------------------------------------------------- quality runs (test #8)
def test_quality_run_clean_then_detects_violations(engine, pg_dsn):
    load(engine, pg_dsn, [P(n) for n in range(1, 6)])
    fid = q(pg_dsn, "SELECT file_id FROM ods.raw_file_manifest")[0][0]
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        _, rows = run_all(c)
        by = {r["rule_id"]: r for r in rows}
        assert not has_blocking_failure(rows)
        assert by["c_title"]["failed"] == 0 and by["c_title"]["passed"] is None          # metric: no pass/fail
        assert by["v_doi_format"]["total"] == 5 and by["t_hours_since_last_ingest"]["details"]["hours"] is not None
        assert q(pg_dsn, "SELECT count(*) FROM meta.quality_result") == [(len(rows),)]
        # inject violations that DB constraints do not prevent
        c.execute("UPDATE dwd.paper SET reference_count = -5 WHERE primary_doi = 'dbt.1' OR primary_doi LIKE '%dbt.1'")
        c.execute("UPDATE dwd.paper SET issued_year = 3000, issns = '{bad}' WHERE primary_doi LIKE '%dbt.2'")
        c.execute("UPDATE dwd.paper SET author_count = 9 WHERE primary_doi LIKE '%dbt.3'")
        c.execute("UPDATE dwd.paper SET primary_doi = 'not-a-doi' WHERE primary_doi LIKE '%dbt.4'")
        c.execute("INSERT INTO ods.crossref_works(doi_norm,source,source_id,payload,payload_hash,batch_id,raw_file_id,raw_line) "
                  "VALUES ('10.99999/orphan','crossref','x','{}','h','fx',%s,0)", (fid,))
        _, rows = run_all(c)
    by = {r["rule_id"]: r for r in rows}
    assert has_blocking_failure(rows)
    assert by["v_reference_count_non_negative"]["failed"] == 1 and by["v_reference_count_non_negative"]["passed"] is False
    assert by["v_issued_year_range"]["failed"] == 1 and by["v_issued_year_range"]["severity"] == "warning"
    assert by["v_issn_format"]["failed"] == 1
    assert by["s_author_count_matches"]["failed"] == 1
    assert by["v_doi_format"]["failed"] == 1
    assert by["s_ods_dwd_coverage"]["failed"] >= 1           # ODS row with neither paper nor quarantine record
    assert by["r_paper_has_doi_identifier"]["failed"] >= 1   # primary_doi was changed away from its identifier
    # results of both runs are kept (append-only execution records)
    assert q(pg_dsn, "SELECT count(DISTINCT run_id) FROM meta.quality_result") == [(2,)]


def test_daily_report_is_generated_from_recorded_results(engine, pg_dsn):
    load(engine, pg_dsn, [P(1), P(2, DOI="bad")])
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        run_all(c)
        text = build_report(c)
    assert "Error rules failing: **0**" in text and "title completeness" in text and "`ingest_doi_format`" in text
    assert "| `doi_format` | open | 1 |" in text


# ---------------------------------------------------------------- quarantine replay (test #9)
def test_quarantine_replay_after_fix(engine, pg_dsn):
    bad = P(1, DOI="not a doi")
    r = engine.process([rec(bad), rec(P(2))])
    assert r.quarantined == 1
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        assert replay_quarantine(c, engine) == {"examined": 1, "replayed": 0, "still_failing": 1}   # unchanged -> still bad
        assert q(pg_dsn, "SELECT status, last_replay_error IS NOT NULL FROM meta.quarantine_record") == [("open", True)]
        fixed = dict(bad, DOI="10.99999/fixed.1")                                              # repair the payload
        c.execute("UPDATE meta.quarantine_record SET payload = %s", (json.dumps(fixed),))
        assert replay_quarantine(c, engine) == {"examined": 1, "replayed": 1, "still_failing": 0}
        assert replay_quarantine(c, engine)["examined"] == 0                                    # nothing left; idempotent
    assert q(pg_dsn, "SELECT status, replayed_at IS NOT NULL FROM meta.quarantine_record") == [("replayed", True)]
    assert q(pg_dsn, "SELECT primary_doi FROM dwd.paper ORDER BY 1") == [("10.99999/dbt.2",), ("10.99999/fixed.1",)]
    assert q(pg_dsn, "SELECT count(*) FROM meta.quarantine_record") == [(1,)]                   # replay never re-quarantines


# ---------------------------------------------------------------- dbt
def dbt(dsn_db: str, tmp: Path, *args: str, check=True):
    exe = Path(sys.executable).parent / "dbt"
    env = {**os.environ, "PG_DB": dsn_db}
    cmd = [str(exe), *args, "--profiles-dir", str(ROOT / "dbt"), "--project-dir", str(ROOT / "dbt"),
           "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")]
    r = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=ROOT / "dbt")
    if check and r.returncode:
        pytest.fail(f"dbt {' '.join(args)} failed:\n{r.stdout[-3000:]}\n{r.stderr[-1000:]}")
    return r


def dbname(dsn):
    return dsn.split("dbname=")[1]


def snapshot(dsn):
    """Content of every incremental/table model, minus load timestamps, in a stable order."""
    out = {}
    for t, order in [("dws.data_quality_daily", "stat_date"), ("dws.paper_change_daily", "change_date"),
                     ("dws.journal_statistics", "journal_key"), ("dws.publisher_statistics", "publisher_key"),
                     ("dws.publication_year_statistics", "issued_year"), ("dim.journal", "journal_key"),
                     ("dim.publisher", "publisher_key")]:
        out[t] = [r[0] for r in q(dsn, f"SELECT (to_jsonb(t) - '_loaded_at')::text FROM {t} t ORDER BY {order}")]
    return out


def seed_history(dsn):
    """Spread existing rows over past days so the incremental watermark logic is really exercised."""
    ex(dsn, """UPDATE dwd.paper SET created_at = timestamptz '2026-01-05' + (random() * 2 || ' days')::interval,
               updated_at = timestamptz '2026-01-05' WHERE true""")
    ex(dsn, "UPDATE dwd.paper_version v SET created_at = p.created_at FROM dwd.paper p WHERE p.paper_id = v.paper_id")


def test_dbt_build_dimensions_are_deterministic_and_tests_pass(engine, pg_dsn, tmp_path):
    ps = [P(1, ISSN=["1111-1111", "2222-2222"], **{"container-title": ["Same Name"]}),   # print + electronic
          P(2, ISSN=["2222-2222"], **{"container-title": ["Same Name"]}),                # electronic only -> same journal
          P(3, ISSN=["3333-3333"], **{"container-title": ["Same Name"]}),                # same NAME, other ISSN -> different journal
          P(4, ISSN=[], member="7", publisher="Other Press"),                            # no ISSN -> no journal
          P(5, ISSN=["1111-1111"], title="Another journal paper")]
    load(engine, pg_dsn, ps)
    dbt(dbname(pg_dsn), tmp_path, "build")
    j = dict(q(pg_dsn, "SELECT k.journal_key, count(*) FROM int.int_paper_keys k GROUP BY 1"))
    assert j == {"1111-1111": 3, "3333-3333": 1, None: 1}
    assert q(pg_dsn, "SELECT journal_key, issns FROM dim.journal ORDER BY 1") == [("1111-1111", ["1111-1111", "2222-2222"]), ("3333-3333", ["3333-3333"])]
    assert q(pg_dsn, "SELECT count(*) FROM dim.journal WHERE journal_title = 'Same Name'") == [(2,)]   # names never merge journals
    assert dict(q(pg_dsn, "SELECT publisher_key, paper_count FROM dws.publisher_statistics")) == {"42": 4, "7": 1}
    assert q(pg_dsn, "SELECT paper_count FROM dws.publication_year_statistics") == [(5,)]
    assert q(pg_dsn, "SELECT papers, title_rate FROM dws.data_quality_daily")[0][0] == 5
    # search model is usable
    assert q(pg_dsn, "SELECT count(*) FROM ads.paper_search WHERE search_tsv @@ plainto_tsquery('simple', 'Lovelace')") == [(5,)]
    assert q(pg_dsn, "SELECT papers, raw_records FROM ads.paper_overview") == [(5, 5)]


def test_dbt_data_tests_catch_broken_data(engine, pg_dsn, tmp_path):
    load(engine, pg_dsn, [P(n) for n in range(1, 4)])
    dbt(dbname(pg_dsn), tmp_path, "build")
    ex(pg_dsn, "UPDATE dwd.paper SET cited_by_count = -1 WHERE primary_doi LIKE '%dbt.1'")
    ex(pg_dsn, "DELETE FROM dwd.paper_author WHERE ordinal = 0")
    r = dbt(dbname(pg_dsn), tmp_path, "test", check=False)
    assert r.returncode != 0
    assert "non_negative_stg_paper_cited_by_count" in r.stdout and "FAIL" in r.stdout


def test_dbt_incremental_equals_full_refresh(engine, pg_dsn, tmp_path):  # test #11
    batch1 = [rec(P(n, ISSN=[f"{n % 4:04d}-0000".replace("0000", "1234")] if n % 5 else [], member=str(n % 3))) for n in range(1, 21)]
    load(engine, pg_dsn, [r.payload for r in batch1])
    seed_history(pg_dsn)
    dbt(dbname(pg_dsn), tmp_path, "run")                                     # initial build of everything

    # new day of data: new papers, an update to an OLD paper (touches an old stat_date), a new quarantined record, a replay-able one
    new = [rec(P(n, ISSN=["9999-9999"], member="3")) for n in range(21, 31)]
    changed = rec(P(3, ISSN=[], title="Changed title", indexed="2026-09-01T00:00:00Z", member="1"))
    load(engine, pg_dsn, [r.payload for r in new] + [changed.payload, P(99, DOI="broken")])
    dbt(dbname(pg_dsn), tmp_path, "run")                                     # incremental
    inc = snapshot(pg_dsn)
    assert len(inc["dws.data_quality_daily"]) >= 2 and len(inc["dws.paper_change_daily"]) >= 2

    dbt(dbname(pg_dsn), tmp_path, "run", "--full-refresh")                   # ground truth
    full = snapshot(pg_dsn)
    assert inc == full, "incremental result diverged from full refresh"

    dbt(dbname(pg_dsn), tmp_path, "run")                                     # idempotent no-op incremental
    assert snapshot(pg_dsn) == full
    dbt(dbname(pg_dsn), tmp_path, "test")
