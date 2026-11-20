import csv
import io
import json

import psycopg
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from paperforge.api.app import create_app
from paperforge.cli import app as cli
from paperforge.config.settings import get_settings

pytestmark = pytest.mark.e2e
RUNNER = CliRunner()


def cmd(*args, expect=0):
    r = RUNNER.invoke(cli, list(args))
    assert r.exit_code == expect, f"{' '.join(args)} -> exit {r.exit_code}\n{r.output[-2500:]}\n{r.exception!r}"
    return r.output


def q(dsn, sql, *a):
    with psycopg.connect(dsn) as c:
        return c.execute(sql, a or None).fetchall()


@pytest.fixture(scope="module")
def built(warehouse):
    """The real pipeline, end to end, through the real CLI: Kafka -> worker -> dbt -> quality -> lineage -> catalog."""
    dsn = warehouse["dsn"]
    assert warehouse["ingest"].status == "completed"
    cmd("events", "init-topics")
    out = cmd("events", "publish")
    assert "'failed': 0" in out
    out = cmd("worker", "run", "--idle-exit", "6")                  # streaming: latest record per DOI
    assert "dlq=0" in out
    cmd("transform")                                                  # batch replay of the raw history (older/late versions)
    cmd("dbt-build")
    cmd("quality", "load-dbt", "--path", str(get_settings().dbt_target_path + "/run_results.json"))
    cmd("quality", "run")
    cmd("lineage", "sync-dbt")
    cmd("catalog", "refresh")
    cmd("quality", "report")
    return warehouse


@pytest.fixture(scope="module")
def api(built):
    with TestClient(create_app(built["dsn"])) as c:
        yield c


def get(api, url, **kw):
    r = api.get(url, **kw)
    assert r.status_code == 200, f"{url} -> {r.status_code} {r.text[:300]}"
    return r.json()


def pid(api, n):
    return get(api, f"/api/v1/papers?doi=10.99999/e2e.{n}")["items"][0]["paper_id"]


# ------------------------------------------------------------------ pipeline result
def test_pipeline_produced_consistent_layers(built):
    dsn = built["dsn"]
    r = built["ingest"]
    assert (r.rows_seen, r.rows_inserted, r.rows_updated, r.rows_unchanged) == (30, 25, 3, 2)   # 3 newer versions; late v1 + duplicate ignored
    assert q(dsn, "SELECT count(*) FROM ods.crossref_works") == [(25,)]                          # 24 valid papers + the invalid-DOI record
    assert q(dsn, "SELECT count(*) FROM dwd.paper") == [(24,)]
    assert q(dsn, "SELECT count(DISTINCT source_id) FROM meta.quarantine_record WHERE status='open'") == [(1,)]
    assert q(dsn, "SELECT count(*) FROM ods.crossref_works WHERE published_at IS NULL") == [(0,)]   # everything was announced
    assert q(dsn, "SELECT sum(record_count) FROM ods.raw_file_manifest") == [(30,)]                  # raw keeps all 30 source records


def test_all_dbt_tests_and_quality_gate_passed(built):
    dsn = built["dsn"]
    assert q(dsn, "SELECT count(*) FROM meta.quality_result r JOIN meta.quality_rule q ON q.rule_id=r.rule_id AND q.version=r.rule_version "
                  "WHERE q.engine='dbt' AND r.passed IS FALSE") == [(0,)]
    assert q(dsn, "SELECT count(*) FROM ads.quality_overview WHERE severity='error' AND passed IS FALSE") == [(0,)]
    assert q(dsn, "SELECT status FROM meta.pipeline_run WHERE dag_id='cli' ORDER BY started_at") and \
        {r[0] for r in q(dsn, "SELECT status FROM meta.pipeline_run")} == {"success"}


# ------------------------------------------------------------------ /papers
def test_papers_list_filters_pagination_and_sorting(api, built):
    dsn = built["dsn"]
    all_ = get(api, "/api/v1/papers?page_size=200")
    n = q(dsn, "SELECT count(*) FROM dwd.paper")[0][0]
    assert all_["total"] == n == len(all_["items"])
    p1, p2 = get(api, "/api/v1/papers?page_size=10&page=1&sort=title_asc"), get(api, "/api/v1/papers?page_size=10&page=2&sort=title_asc")
    assert {i["paper_id"] for i in p1["items"]}.isdisjoint({i["paper_id"] for i in p2["items"]}) and p1["total"] == n
    assert [i["title"] for i in p1["items"]] == sorted(i["title"] for i in p1["items"])
    # DOI exact, tolerant to URL/case variants
    assert get(api, "/api/v1/papers?doi=https://doi.org/10.99999/E2E.7")["total"] == 1
    assert get(api, "/api/v1/papers?doi_prefix=10.99999/e2e.1")["total"] == q(dsn, "SELECT count(*) FROM dwd.paper WHERE primary_doi LIKE '10.99999/e2e.1%'")[0][0]
    # title, journal (name and ISSN-derived key), publisher (name and member id), year(s)
    assert get(api, "/api/v1/papers?title=protein folding")["total"] == n
    assert get(api, "/api/v1/papers?title=no such words")["total"] == 0
    assert get(api, "/api/v1/papers?journal=Computational")["total"] == q(dsn, "SELECT count(*) FROM dwd.paper WHERE container_title='Computational Testing'")[0][0]
    jk = get(api, "/api/v1/papers?journal=Journal of Tests")["items"][0]["journal_key"]
    assert jk == "1111-1111" and get(api, f"/api/v1/papers?journal={jk}")["total"] == get(api, "/api/v1/papers?journal=Journal of Tests")["total"]
    assert get(api, "/api/v1/papers?publisher=Bio Publishing")["total"] == get(api, "/api/v1/papers?publisher=91")["total"] > 0
    y = get(api, "/api/v1/papers?year=2018")
    assert y["total"] == q(dsn, "SELECT count(*) FROM dwd.paper WHERE issued_year=2018")[0][0] and all(i["issued_year"] == 2018 for i in y["items"])
    assert get(api, "/api/v1/papers?year_from=2017&year_to=2019")["total"] == q(dsn, "SELECT count(*) FROM dwd.paper WHERE issued_year BETWEEN 2017 AND 2019")[0][0]
    assert get(api, "/api/v1/papers?q=Lovelace&publisher=Test Press")["total"] == get(api, "/api/v1/papers?publisher=Test Press")["total"] - \
        q(dsn, "SELECT count(*) FROM dwd.paper p WHERE publisher='Test Press' AND author_count=0")[0][0]
    # bad input is rejected, not crashed
    assert api.get("/api/v1/papers?page_size=1000").status_code == 422 and api.get("/api/v1/papers?sort=evil;DROP").status_code == 422
    assert get(api, "/api/v1/papers?title=%27;DROP TABLE dwd.paper;--")["total"] == 0 and q(dsn, "SELECT count(*) FROM dwd.paper")[0][0] == n


def test_export_csv_and_jsonl_match_search(api):
    total = get(api, "/api/v1/papers?publisher=Test Press")["total"]
    r = api.get("/api/v1/papers/export?publisher=Test Press&format=csv")
    assert r.headers["content-type"].startswith("text/csv") and "papers.csv" in r.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert len(rows) == total and set(rows[0]) >= {"doi", "title", "journal_title", "publisher", "issued_year"}
    lines = api.get("/api/v1/papers/export?publisher=Test Press&format=jsonl&limit=3").text.strip().splitlines()
    assert len(lines) == min(3, total) and json.loads(lines[0])["publisher"] == "Test Press"


# ------------------------------------------------------------------ /papers/{id}
def test_paper_detail_has_standardized_and_raw_views(api):
    d = get(api, f"/api/v1/papers/{pid(api, 6)}")
    assert d["doi"] == "10.99999/e2e.6" and d["title"].startswith("Study 6 of protein folding") and d["type"] == "journal-article"
    assert d["authors"][0]["family"] == "Lovelace" and d["authors"][0]["orcid"] == "0000-0002-1825-0097"
    assert d["references"][0]["ref_doi"] == "10.1/ref" and d["identifiers"][0]["scheme"] == "doi" and d["identifiers"][0]["status"] == "active"
    assert d["raw"]["payload"]["DOI"] == "10.99999/E2E.6" and d["raw"]["payload"]["title"][0] == d["title"]     # raw keeps source casing
    assert d["normalized"]["doi"] == "10.99999/e2e.6" and d["raw"]["raw_file_id"] is not None
    assert api.get("/api/v1/papers/00000000-0000-0000-0000-000000000000").status_code == 404
    assert api.get("/api/v1/papers/xyz").status_code == 422


def test_versions_history_with_late_and_newer_versions(api):
    v = get(api, f"/api/v1/papers/{pid(api, 2)}/versions")["versions"]          # arrived: v1, v3, then v1-again (late; identical to first)
    assert [x["source_updated_at"][:10] for x in v] == ["2026-01-01", "2026-03-01"]
    assert [x["is_current"] for x in v] == [False, True] and v[0]["valid_to"] == v[1]["valid_from"]
    assert v[1]["snapshot"]["title"].endswith("v3") and "title" in v[1]["changed_fields"] and v[0]["changed_fields"] == []
    assert get(api, f"/api/v1/papers/{pid(api, 2)}")["title"].endswith("v3")      # current = newest on the source timeline
    assert len(get(api, f"/api/v1/papers/{pid(api, 1)}/versions")["versions"]) == 1   # exact duplicate created nothing


# ------------------------------------------------------------------ statistics / quality / pipelines
def test_overview_numbers_equal_database_facts(api, built):
    dsn, o = built["dsn"], get(api, "/api/v1/statistics/overview")
    assert o["papers"] == q(dsn, "SELECT count(*) FROM dwd.paper")[0][0] and o["raw_records"] == q(dsn, "SELECT count(*) FROM ods.crossref_works")[0][0]
    assert o["raw_file_rows"] == q(dsn, "SELECT sum(record_count) FROM ods.raw_file_manifest")[0][0]
    assert o["open_quarantine"] == 1 and o["versions"] == o["papers"] + 3                   # 3 papers have two versions
    assert sum(r["paper_count"] for r in o["by_year"]) == o["papers"] and 0 < o["quality_pass_rate"] <= 1
    assert sum(r["new_papers"] for r in o["daily_changes"]) == o["papers"]
    assert {j["journal_title"] for j in o["top_journals"]} == {"Journal of Tests", "Computational Testing", "Bio Reports"}


def test_quality_endpoint_reports_rules_metrics_quarantine(api, built):
    d = get(api, "/api/v1/quality/metrics")
    ids = {r["rule_id"]: r for r in d["rules"]}
    assert ids["v_doi_format"]["passed"] is True and ids["ingest_doi_format"]["failed_rows"] == 1 and {"dbt", "pandera", "sql", "python-validator"} <= {r["engine"] for r in d["rules"]}
    m = {x["rule_id"]: x for x in d["metrics"]}
    n_abs = q(built["dsn"], "SELECT count(*) FROM dwd.paper WHERE abstract IS NOT NULL")[0][0]
    n = q(built["dsn"], "SELECT count(*) FROM dwd.paper")[0][0]
    assert m["c_abstract"]["coverage"] == round(n_abs / n, 4) and m["c_title"]["coverage"] == 1.0
    assert [x["rule_id"] for x in d["quarantine"]] == ["doi_format"] and d["quarantine"][0]["payload"]["DOI"] == "not-a-valid-doi" and d["quarantine"][0]["status"] == "open"
    assert {x["dimension"] for x in d["dimensions"]} >= {"completeness", "uniqueness", "validity", "consistency", "referential_integrity", "source_completeness"}
    assert d["daily"] and d["daily"][-1]["papers"] == n


def test_pipelines_runs_show_real_task_records(api):
    d = get(api, "/api/v1/pipelines/runs?limit=50")
    tasks = {t["task_id"] for r in d["runs"] for t in r["tasks"]}
    assert {"publish", "stream_worker", "transform", "dbt_build", "quality_run", "lineage_sync_dbt", "catalog_refresh"} <= tasks
    st = {s["task_id"]: s for s in d["stages"]}
    assert st["stream_worker"]["rows_in"] == 25 and st["dbt_build"]["status"] == "success" and st["quality_run"]["rows_failed"] == 0
    assert d["batches"][0]["rows_seen"] == 30 and d["batches"][0]["rows_unchanged"] == 2 and d["batches"][0]["status"] == "completed"
    assert all(r["status"] == "success" for r in d["runs"])


def test_reconciliation_endpoint_uses_duckdb_over_parquet(api):
    d = get(api, "/api/v1/quality/reconciliation")
    assert d["parquet_available"] and all(d["checks"].values()), d


# ------------------------------------------------------------------ lineage / catalog
def test_lineage_graph_and_traces(api):
    g = get(api, "/api/v1/datasets/lineage")
    ids = {n["id"] for n in g["nodes"]}
    assert {"raw.crossref_jsonl", "ods.crossref_works", "kafka.paper.raw.events", "dwd.paper", "dws.journal_statistics", "ads.paper_search"} <= ids
    up = get(api, "/api/v1/datasets/lineage?dataset=ads.paper_search&direction=up")
    assert {"dwd.paper", "kafka.paper.raw.events", "ods.crossref_works", "raw.crossref_jsonl"} <= {e["upstream"] for e in up["edges"]}
    down = get(api, "/api/v1/datasets/lineage?dataset=ods.crossref_works&direction=down")
    assert "ads.paper_search" in {e["downstream"] for e in down["edges"]}


def test_catalog_lists_layers_with_real_counts_and_columns(api, built):
    cat = {d["dataset_id"]: d for d in get(api, "/api/v1/datasets")}
    assert {"ods", "dwd", "dim", "dws", "ads", "meta"} <= {d["layer"] for d in cat.values()}
    assert cat["dwd.paper"]["row_count"] == q(built["dsn"], "SELECT count(*) FROM dwd.paper")[0][0]
    one = get(api, "/api/v1/datasets/dwd.paper")
    cols = {c["name"]: c for c in one["columns"]}
    assert cols["primary_doi"]["type"] == "text" and cols["paper_id"]["nullable"] is False and one["primary_key"] == "paper_id"
    assert "raw.crossref_jsonl" in one["upstream"] and "stg.stg_paper" in one["downstream"]
    assert api.get("/api/v1/datasets/nope.nope").status_code == 404


def test_every_endpoint_documented_in_openapi(api):
    paths = set(get(api, "/openapi.json")["paths"])
    assert {"/api/v1/papers", "/api/v1/papers/{paper_id}", "/api/v1/papers/{paper_id}/versions", "/api/v1/statistics/overview",
            "/api/v1/quality/metrics", "/api/v1/pipelines/runs", "/api/v1/datasets/lineage"} <= paths


# ------------------------------------------------------------------ isolation + identifier change (requirement: test data never mixes with real stats)
def test_external_identifier_change_is_representable_and_isolated(api, built):
    dsn = built["dsn"]
    p = get(api, f"/api/v1/papers/{pid(api, 8)}")
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("UPDATE dwd.paper_identifier SET valid_to = now(), status='retired' WHERE paper_id=%s AND scheme='doi'", (p["paper_id"],))
        c.execute("INSERT INTO dwd.paper_identifier(paper_id, scheme, value, source, valid_from) VALUES (%s,'doi','10.99999/e2e.8-moved','crossref', now())", (p["paper_id"],))
        c.execute("INSERT INTO dwd.paper_identity_event(event_id, event_type, from_paper_id, to_paper_id, payload, occurred_at) "
                  "VALUES (gen_random_uuid(), 'identifier_change', %s, %s, %s, now())", (p["paper_id"], p["paper_id"], json.dumps({"old": p["doi"], "new": "10.99999/e2e.8-moved"})))
        with pytest.raises(psycopg.errors.UniqueViolation):          # one ACTIVE owner per identifier
            c.execute("INSERT INTO dwd.paper_identifier(paper_id, scheme, value, source, valid_from) SELECT paper_id, 'doi', '10.99999/e2e.8-moved', 'crossref', now() FROM dwd.paper WHERE primary_doi LIKE '%%e2e.9'")
    d = get(api, f"/api/v1/papers/{p['paper_id']}")
    assert [(i["value"], i["status"]) for i in d["identifiers"]] == [("10.99999/e2e.8", "retired"), ("10.99999/e2e.8-moved", "active")]   # history kept
    # the REAL warehouse never saw any test paper
    real = get_settings()
    with psycopg.connect(real.pg_dsn.replace(f"dbname={built['name']}", "dbname=paperforge")) as c:
        assert c.execute("SELECT count(*) FROM dwd.paper WHERE primary_doi LIKE '10.99999/%%'").fetchone()[0] == 0
        assert c.execute("SELECT count(*) FROM ods.crossref_works WHERE doi_norm LIKE '10.99999/%%'").fetchone()[0] == 0
