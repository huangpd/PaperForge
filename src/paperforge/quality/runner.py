import uuid
from datetime import UTC, datetime

import pandera.polars as pa
import polars as pl
import psycopg
from psycopg.types.json import Jsonb

from paperforge.quality.rules import ALL_RULES, PANDERA_RULES, SQL_RULES, TIMELINESS, VALIDATOR_CATALOG, Rule

PAPER_COLS = {"paper_key": pl.Utf8, "primary_doi": pl.Utf8, "issued_year": pl.Int32, "issns": pl.List(pl.Utf8), "reference_count": pl.Int32,
              "cited_by_count": pl.Int32, "source_updated_at": pl.Datetime("us", "UTC")}


def sync_rules(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO meta.quality_rule(rule_id, version, dimension, severity, dataset, description, engine) VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (rule_id, version) DO UPDATE SET dimension=EXCLUDED.dimension, severity=EXCLUDED.severity, "
            "dataset=EXCLUDED.dataset, description=EXCLUDED.description, engine=EXCLUDED.engine",
            [(r.rule_id, r.version, r.dimension, r.severity, r.dataset, r.description, r.engine) for r in ALL_RULES])


def load_paper_frame(conn: psycopg.Connection) -> pl.DataFrame:
    cols = list(PAPER_COLS)
    rows = conn.execute(f"SELECT {', '.join(cols)} FROM dwd.paper").fetchall()
    return pl.DataFrame(rows, schema=PAPER_COLS, orient="row")


def run_pandera(df: pl.DataFrame) -> dict[str, tuple[int, int, dict]]:
    """rule_id -> (total, failed, details). One Pandera schema per rule so each failure count is attributable."""
    out = {}
    for r in PANDERA_RULES:
        schema = pa.DataFrameSchema({r.column: pa.Column(PAPER_COLS[r.column], nullable=True,
                                                         checks=pa.Check(lambda d, e=r.expr: d.lazyframe.select(e), name=r.rule_id))})
        failed, details = 0, {}
        try:
            schema.validate(df, lazy=True)
        except pa.errors.SchemaErrors as e:
            fc = e.failure_cases
            failed = fc.filter(pl.col("check") == r.rule_id).height
            other = fc.filter(pl.col("check") != r.rule_id)
            if other.height:                      # structural problem (dtype/missing column) -> counts as failure
                failed += other.height
                details["structural"] = other["check"].unique().to_list()
            details["sample"] = fc.head(3)["failure_case"].to_list()
        out[r.rule_id] = (df.height, failed, details)
    return out


def _validator_results(conn) -> list[tuple[Rule, int, int, dict]]:
    ods = conn.execute("SELECT count(*) FROM ods.crossref_works").fetchone()[0]
    ods_arxiv = conn.execute("SELECT count(*) FROM ods.arxiv_works").fetchone()[0]
    arxiv_versions = conn.execute("SELECT count(*) FROM dwd.paper_version WHERE source = 'arxiv'").fetchone()[0]
    versions = conn.execute("SELECT count(*) FROM dwd.paper_version").fetchone()[0]
    quarantine = dict(conn.execute("SELECT rule_id, count(*) FROM meta.quarantine_record WHERE status='open' GROUP BY 1").fetchall())
    flags = dict(conn.execute("SELECT f->>'rule_id', count(*) FROM dwd.paper_version v, jsonb_array_elements(v.quality_flags) f GROUP BY 1").fetchall())
    res = []
    for r in VALIDATOR_CATALOG:
        name = r.rule_id.removeprefix("ingest_")
        is_arxiv = name.startswith("arxiv_")          # arXiv rules are measured against arXiv rows, not the Crossref totals
        if r.severity == "error":
            res.append((r, ods_arxiv if is_arxiv else ods, quarantine.get(name, 0), {"open_quarantine": quarantine.get(name, 0)}))
        else:
            res.append((r, arxiv_versions if is_arxiv else versions, flags.get(name, 0), {}))
    return res


def run_all(conn: psycopg.Connection) -> tuple[uuid.UUID, list[dict]]:
    sync_rules(conn)
    run_id, now = uuid.uuid4(), datetime.now(UTC)
    results: list[tuple[Rule, int, int, dict]] = []
    pr = run_pandera(load_paper_frame(conn))
    for r in PANDERA_RULES:
        results.append((r, *pr[r.rule_id]))
    for r in SQL_RULES:
        total, failed = conn.execute(r.sql).fetchone()
        results.append((r, total, failed, {}))
    last = conn.execute("SELECT max(ingested_at) FROM ods.crossref_works").fetchone()[0]
    hours = round((now - last).total_seconds() / 3600, 2) if last else None
    results.append((TIMELINESS, 1, 0, {"hours": hours, "last_ingested_at": last.isoformat() if last else None}))
    results += _validator_results(conn)
    rows = []
    with conn.transaction():
        for r, total, failed, details in results:
            passed = None if r.severity == "metric" else failed == 0
            if r.engine == "python-validator" and r.severity == "error":
                # Ingest-time Error rules are a gate, not an audit: failed_rows = records kept OUT of dwd (open quarantine).
                # They count as passed because nothing invalid reached the standardized layer.
                passed = True
                details = {**details, "note": "failed_rows are quarantined, not present in dwd"}
            conn.execute("INSERT INTO meta.quality_result(run_id, rule_id, rule_version, dataset, severity, total_rows, failed_rows, passed, details, run_at) "
                         "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                         (run_id, r.rule_id, r.version, r.dataset, r.severity, total, failed, passed, Jsonb(details), now))
            rows.append({"rule_id": r.rule_id, "dimension": r.dimension, "severity": r.severity, "dataset": r.dataset,
                         "total": total, "failed": failed, "passed": passed, "details": details, "description": r.description})
    return run_id, rows


def has_blocking_failure(rows: list[dict]) -> bool:
    return any(r["severity"] == "error" and r["passed"] is False for r in rows)


from paperforge.quality.base import QualityValidator  # noqa: E402


class PanderaQualityValidator(QualityValidator):
    """Default implementation: Pandera (polars) rules + SQL rules + roll-up of ingest-time validator results."""

    def run(self, conn: psycopg.Connection) -> tuple[uuid.UUID, list[dict]]:
        return run_all(conn)
