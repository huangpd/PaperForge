import os
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from paperforge.lineage.base import Edge
from paperforge.lineage.catalog import refresh
from paperforge.lineage.datasets import RUNTIME_EDGES, register_static
from paperforge.lineage.dbt_manifest import parse_manifest, sync_dbt
from paperforge.lineage.pg_emitter import PgLineageEmitter
from paperforge.lineage.query import path_exists, trace
from paperforge.runs import finish_run, reap_stale, tracked

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def db(pg_dsn, monkeypatch):
    """Point the code under test (which reads settings from the environment) at the isolated database."""
    for k in ("PF_DAG_ID", "PF_RUN_ID", "PF_TASK_ID", "PF_TRY_NUMBER", "PF_TRIGGER"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("PG_DB", pg_dsn.split("dbname=")[1])
    c = psycopg.connect(pg_dsn, autocommit=True)
    yield c
    c.close()


def rows(c, sql, *a):
    return c.execute(sql, a or None).fetchall()


# ------------------------------------------------------------------------------------- run tracking
def test_tracked_records_success_with_counts_and_runtime_lineage(db):
    with tracked("transform", params={"batch": "b1"}, lineage_task="transform") as h:
        h.set(rows_in=10, rows_out=8, rows_failed=2, data_version="b1", unchanged=0)
    ((task, status, rin, rout, rfail, ver, dur, details),) = rows(
        db, "SELECT task_id, status, rows_in, rows_out, rows_failed, data_version, duration_ms, details FROM meta.task_run")
    assert (task, status, rin, rout, rfail, ver) == ("transform", "success", 10, 8, 2, "b1") and dur >= 0
    assert details["batch"] == "b1" and details["unchanged"] == 0
    assert rows(db, "SELECT dag_id, status, ended_at IS NOT NULL FROM meta.pipeline_run") == [("cli", "success", True)]
    # runtime lineage was registered on success and points at the task run
    assert rows(db, "SELECT count(*) FROM meta.lineage_edge WHERE source='runtime' AND task_id='transform' AND last_task_run_id IS NOT NULL")[0][0] \
        == len(RUNTIME_EDGES["transform"])


def test_failed_task_is_recorded_reraised_and_registers_no_lineage(db):
    with pytest.raises(ValueError, match="boom"):
        with tracked("ingest", lineage_task="ingest") as h:
            h.set(rows_in=5)
            raise ValueError("boom")
    assert rows(db, "SELECT status, rows_in, error FROM meta.task_run") == [("failed", 5, "ValueError: boom")]
    assert rows(db, "SELECT status FROM meta.pipeline_run") == [("failed",)]
    assert rows(db, "SELECT count(*) FROM meta.lineage_edge") == [(0,)]          # data did not actually move


def test_orchestrated_run_groups_tasks_tracks_retries_and_finishes(db, monkeypatch):
    env = {"PF_DAG_ID": "crossref_incremental", "PF_RUN_ID": "scheduled__2026-11-19", "PF_TRIGGER": "scheduled"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("PF_TASK_ID", "sync")
    for attempt in (1, 2):                                       # attempt 1 fails, attempt 2 succeeds (Airflow retry)
        monkeypatch.setenv("PF_TRY_NUMBER", str(attempt))
        try:
            with tracked("sync") as h:
                if attempt == 1:
                    raise ConnectionError("api down")
                h.set(rows_in=3, rows_out=3)
        except ConnectionError:
            pass
    monkeypatch.setenv("PF_TASK_ID", "publish_outbox")
    monkeypatch.setenv("PF_TRY_NUMBER", "1")
    with tracked("publish") as h:
        h.set(rows_out=3)
    assert rows(db, "SELECT count(*) FROM meta.pipeline_run") == [(1,)]                          # grouped under ONE run
    assert rows(db, "SELECT task_id, attempt, status FROM meta.task_run ORDER BY started_at") == [
        ("sync", 1, "failed"), ("sync", 2, "success"), ("publish_outbox", 1, "success")]
    assert finish_run() == "success"                                                              # latest attempt per task decides
    assert rows(db, "SELECT status, trigger FROM meta.pipeline_run") == [("success", "scheduled")]


def test_run_fails_when_latest_attempt_of_a_task_failed(db, monkeypatch):
    monkeypatch.setenv("PF_DAG_ID", "d"); monkeypatch.setenv("PF_RUN_ID", "r1"); monkeypatch.setenv("PF_TASK_ID", "t")
    with pytest.raises(RuntimeError):
        with tracked("sync"):
            raise RuntimeError("x")
    assert finish_run() == "failed"


def test_reap_marks_killed_tasks_failed_but_spares_long_running_workers(db):
    with tracked("transform") as h:
        pass
    db.execute("INSERT INTO meta.pipeline_run(run_id, dag_id, status, started_at) VALUES ('00000000-0000-0000-0000-000000000001','cli','running', now() - interval '5 hours')")
    for tid, task in (("00000000-0000-0000-0000-0000000000a1", "ingest"), ("00000000-0000-0000-0000-0000000000a2", "stream_worker")):
        db.execute("INSERT INTO meta.task_run(task_run_id, run_id, task_id, status, started_at) VALUES (%s,'00000000-0000-0000-0000-000000000001',%s,'running', now() - interval '5 hours')", (tid, task))
    out = reap_stale(120)
    assert out["tasks_marked_failed"] == 1
    assert dict(rows(db, "SELECT task_id, status FROM meta.task_run WHERE task_id IN ('ingest','stream_worker')")) == {"ingest": "failed", "stream_worker": "running"}
    assert "abandoned" in rows(db, "SELECT error FROM meta.task_run WHERE task_id='ingest'")[0][0]
    assert rows(db, "SELECT status FROM meta.pipeline_run WHERE run_id='00000000-0000-0000-0000-000000000001'") == [("running",)]   # worker still alive


# ------------------------------------------------------------------------------------- lineage emitter
def test_emitter_is_idempotent_and_replace_removes_stale_edges(db):
    em = PgLineageEmitter(db)
    e1, e2 = Edge("ods.crossref_works", "dwd.paper", "t"), Edge("dwd.paper", "stg.stg_paper", "m", "dbt")
    em.emit_edges([e1, e1, e2])
    em.emit_edges([e1])
    assert rows(db, "SELECT count(*) FROM meta.lineage_edge") == [(2,)]
    new = [Edge("dwd.paper", "stg.stg_paper", "m2", "dbt"), Edge("dwd.paper", "dim.journal", "m3", "dbt")]
    em.emit_edges([Edge("old.a", "old.b", "m0", "dbt")])
    removed = em.replace_edges("dbt", new)
    assert removed == 1                                                       # old.a -> old.b gone; runtime edge untouched
    assert {(a, b) for a, b in rows(db, "SELECT upstream, downstream FROM meta.lineage_edge")} == {
        ("ods.crossref_works", "dwd.paper"), ("dwd.paper", "stg.stg_paper"), ("dwd.paper", "dim.journal")}
    with pytest.raises(psycopg.errors.CheckViolation):
        em.emit_edges([Edge("a.b", "a.b", "x")])


@pytest.fixture(scope="module")
def manifest(tmp_path_factory):
    """Real manifest of the real dbt project, produced by `dbt parse` (no database access needed)."""
    tmp = tmp_path_factory.mktemp("dbtparse")
    exe = Path(sys.executable).parent / "dbt"
    r = subprocess.run([str(exe), "parse", "--profiles-dir", str(ROOT / "dbt"), "--project-dir", str(ROOT / "dbt"),
                        "--target-path", str(tmp / "t"), "--log-path", str(tmp / "l")], capture_output=True, text=True, cwd=ROOT / "dbt")
    assert r.returncode == 0, r.stdout[-1500:]
    return tmp / "t" / "manifest.json"


def test_dbt_manifest_edges_are_complete_and_use_physical_names(manifest):
    edges, info = parse_manifest(manifest)
    pairs = {(e.upstream, e.downstream) for e in edges}
    assert ("dwd.paper", "stg.stg_paper") in pairs and ("stg.stg_paper", "int.int_paper_keys") in pairs
    assert ("int.int_paper_keys", "dim.journal") not in pairs                      # dim.journal is built from the issn map, not paper keys
    assert ("int.int_issn_journal", "dim.journal") in pairs and ("dim.journal", "dws.journal_statistics") not in pairs
    assert {"dim.journal", "dim.publisher", "dim.date", "dws.journal_statistics", "dws.publisher_statistics",
            "dws.publication_year_statistics", "dws.data_quality_daily", "dws.paper_change_daily",
            "ads.paper_search", "ads.paper_overview", "ads.quality_overview"} <= set(info)    # documented table names
    downstream_of = {d for _, d in pairs}
    assert [m for m in info if m not in downstream_of] == ["dim.date"]            # only the generated calendar has no upstream
    assert info["dws.data_quality_daily"]["materialization"] == "incremental"
    assert info["dws.data_quality_daily"]["meta"]["primary_key"] == "stat_date"     # grain/PK/update strategy declared for each model
    assert all(i["meta"].get("grain") and i["meta"].get("update_strategy") for i in info.values())


def test_end_to_end_chain_crossref_to_ads_and_catalog(db, manifest):
    em = PgLineageEmitter(db)
    register_static(db)
    for task, es in RUNTIME_EDGES.items():
        em.emit_edges([Edge(u, d, task) for u, d in es])
    out = sync_dbt(db, em, manifest)
    assert out["models"] >= 17 and out["edges"] >= 30
    assert path_exists(db, "external.crossref_api", "ads.paper_search")
    assert path_exists(db, "external.crossref_parquet", "dws.journal_statistics")
    assert path_exists(db, "ods.crossref_works", "kafka.paper.raw.events") and path_exists(db, "kafka.paper.raw.events", "dwd.paper")
    assert path_exists(db, "dwd.paper", "dws.data_quality_daily") and path_exists(db, "dws.publication_year_statistics", "dws.publication_year_statistics")
    assert not path_exists(db, "ads.paper_search", "external.crossref_api")           # direction matters
    up = {e["upstream"] for e in trace(db, "ads.paper_search", "up")}
    assert {"dwd.paper", "stg.stg_paper", "external.crossref_api", "raw.crossref_jsonl"} <= up
    down = {e["downstream"] for e in trace(db, "ods.crossref_works", "down")}
    assert {"kafka.paper.raw.events", "dwd.paper", "ads.paper_search"} <= down
    # catalog metadata from dbt
    assert rows(db, "SELECT layer, kind, materialization, grain FROM meta.dataset WHERE dataset_id='dim.journal'") == [
        ("dim", "table", "table", "one journal key (ISSN-derived)")]
    assert rows(db, "SELECT kind, materialization FROM meta.dataset WHERE dataset_id='stg.stg_paper'") == [("view", "view")]


def test_catalog_refresh_reports_real_counts_types_and_unbuilt(db):
    register_static(db)
    db.execute("INSERT INTO ods.ingestion_batch(batch_id,source,mode,status,params) VALUES ('b','crossref','bootstrap','completed','{}')")
    PgLineageEmitter(db).emit_edges([Edge("dwd.paper", "dim.never_built", "m", "dbt")])       # registered but not built
    out = refresh(db)
    assert out["not_built"] == 1 and out["refreshed"] >= 15       # static datasets exist; dbt ones do not in this DB
    row = rows(db, "SELECT row_count, columns, last_data_at IS NOT NULL, refreshed_at IS NOT NULL FROM meta.dataset WHERE dataset_id='ods.ingestion_batch'")[0]
    assert row[0] == 1 and row[2] and row[3]
    cols = {c["name"]: c for c in row[1]}
    assert cols["batch_id"]["type"] == "text" and cols["batch_id"]["nullable"] is False and cols["params"]["type"] == "jsonb"
    assert rows(db, "SELECT row_count FROM meta.dataset WHERE dataset_id='dwd.paper'") == [(0,)]       # empty table: count is 0, not NULL
    assert rows(db, "SELECT row_count FROM meta.dataset WHERE dataset_id='dim.never_built'") == [(None,)]
