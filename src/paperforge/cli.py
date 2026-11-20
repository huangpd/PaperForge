import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import typer

from paperforge.config.settings import get_settings
from paperforge.connectors.crossref_api import CrossrefApiConnector
from paperforge.connectors.crossref_parquet import CrossrefParquetConnector
from paperforge.db import connect, migrate
from paperforge.ingestion.parquet_export import export_batch
from paperforge.ingestion.runner import IngestionRunner
from paperforge.storage.local import LocalStorage

app = typer.Typer(no_args_is_help=True)

import functools  # noqa: E402
import threading  # noqa: E402

_CUR = threading.local()


def cur():
    """Handle of the task_run opened by @tracked_cmd for the running command."""
    return _CUR.h


def tracked_cmd(task_id: str, lineage: Optional[str] = None):
    """Record pipeline_run/task_run (+ runtime lineage on success) around a CLI command."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            from paperforge.runs import tracked
            with tracked(task_id, params={k: v for k, v in kw.items() if isinstance(v, (str, int, float, bool, type(None)))},
                         lineage_task=lineage) as h:
                _CUR.h = h
                return fn(*a, **kw)
        return wrapper
    return deco


@app.callback()
def _main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


@app.command("migrate")
def migrate_cmd():
    with connect() as c:
        typer.echo(f"applied: {migrate(c)}")


def _report(res, storage, parquet=True):
    typer.echo(res)
    cur().set(rows_in=res.rows_seen, rows_out=res.rows_inserted + res.rows_updated, rows_failed=res.rows_skipped,
              data_version=res.batch_id, status=res.status, inserted=res.rows_inserted, updated=res.rows_updated,
              unchanged=res.rows_unchanged, truncated=res.truncated, units_failed=res.units_failed)
    if res.status == "completed" and parquet:
        typer.echo(f"parquet files: {len(export_batch(storage, res.batch_id))}")
    if res.status != "completed":
        raise typer.Exit(1)


@app.command("ingest")
@tracked_cmd("ingest", lineage="ingest")
def ingest(limit: int = typer.Option(None, help="records to ingest (bootstrap)"),
           resume: Optional[str] = typer.Option(None, help="resume an unfinished batch id")):
    """Bootstrap from the local offline Crossref parquet (domain-stratified sample)."""
    s = get_settings()
    conn = CrossrefParquetConnector(s.crossref_parquet_dir, memory_limit=s.duckdb_memory_limit, threads=s.duckdb_threads)
    storage = LocalStorage(s.data_dir)
    runner = IngestionRunner(conn, storage)
    if resume:
        bid = runner.start("bootstrap", [], {}, None, batch_id=resume)
    else:
        if not limit:
            raise typer.BadParameter("--limit is required")
        bid = runner.start("bootstrap", conn.plan(limit=limit), {"files": conn.files, "limit": limit}, None)
    _report(runner.run(bid), storage)


@app.command("sync")
@tracked_cmd("sync", lineage="sync")
def sync(since: Optional[str] = typer.Option(None, help="YYYY-MM-DD; default = watermark - 1 day overlap"),
         until: Optional[str] = typer.Option(None, help="YYYY-MM-DD; default today (UTC)"),
         limit: Optional[int] = None, resume: Optional[str] = None):
    """Incremental/real-API sync by Crossref index-date windows (cursor pagination)."""
    s = get_settings()
    conn = CrossrefApiConnector(s.crossref_api_base, rate_per_sec=s.crossref_rate_per_sec)
    storage = LocalStorage(s.data_dir)
    runner = IngestionRunner(conn, storage, concurrency=s.crossref_concurrency)
    if resume:
        bid = runner.start("incremental", [], {}, None, batch_id=resume)
    else:
        u = date.fromisoformat(until) if until else datetime.now(timezone.utc).date()
        d = date.fromisoformat(since) if since else _watermark(conn.name) - timedelta(days=1)
        bid = runner.start("incremental", conn.plan(since=d, until=u), {"since": str(d), "until": str(u)}, limit)
    res = runner.run(bid)
    if res.truncated:
        typer.echo("batch truncated by --limit: watermark NOT advanced")
    elif res.status == "completed" and not resume:
        _advance_watermark(conn.name, bid)
    _report(res, storage)


@app.command("ingest-arxiv")
@tracked_cmd("ingest_arxiv", lineage="ingest_arxiv")
def ingest_arxiv(file: str = typer.Argument(..., help="arXiv metadata snapshot: .json, or the .zip that contains it"),
                 categories: Optional[str] = typer.Option(None, help="comma list; exact (stat.ML) or prefix (cs.*). Default: core AI categories"),
                 match: str = typer.Option("primary", help="primary = first category only | any = cross-lists count too"),
                 limit: Optional[int] = typer.Option(None, help="stop after N matching records (trial run)"),
                 resume: Optional[str] = typer.Option(None, help="resume an unfinished batch id"),
                 to_dwd: bool = typer.Option(True, "--dwd/--no-dwd", help="after ingestion, standardize the batch into DWD")):
    """Import a scoped slice of the arXiv metadata snapshot: raw JSONL -> ods.arxiv_works -> DWD (batch path, no Kafka).

    Papers with a DOI merge with the same DOI (a Crossref paper only gains an arXiv identifier and dwd.paper_arxiv row);
    papers without a DOI become their own paper keyed 'arxiv:<id>'."""
    import hashlib
    from pathlib import Path

    from paperforge.connectors.arxiv_snapshot import ArxivSnapshotConnector
    path = Path(file).expanduser()
    if not path.is_file():
        raise typer.BadParameter(f"no such file: {path}")
    cats = [c.strip() for c in categories.split(",") if c.strip()] if categories else None
    try:
        conn = ArxivSnapshotConnector(str(path), cats, match)
    except ValueError as e:
        raise typer.BadParameter(str(e))
    s = get_settings()
    storage = LocalStorage(s.data_dir)
    runner = IngestionRunner(conn, storage)
    if resume:
        bid = runner.start("bootstrap", [], {}, None, batch_id=resume)
    else:
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8 << 20), b""):
                h.update(chunk)
        bid = runner.start("bootstrap", conn.plan(), {"file": path.name, "sha256": h.hexdigest(), "categories": conn.categories,
                                                        "match": match}, limit)
    res = runner.run(bid)
    typer.echo(res)
    cur().set(rows_in=res.rows_seen, rows_out=res.rows_inserted + res.rows_updated, rows_failed=res.rows_skipped,
              data_version=res.batch_id, status=res.status, inserted=res.rows_inserted, updated=res.rows_updated,
              unchanged=res.rows_unchanged, truncated=res.truncated)
    if res.status != "completed":
        raise typer.Exit(1)
    if to_dwd:
        out = _transform(bid, 1000)
        typer.echo(out)
        cur().set(dwd_inserted=out.inserted, dwd_updated=out.updated, dwd_unchanged=out.unchanged, dwd_quarantined=out.quarantined)
        typer.echo("note: search and statistics (ads.*, dws.*) only change after `make dbt`.")


@app.command("fetch-doi")
@tracked_cmd("fetch_doi", lineage="fetch_doi")
def fetch_doi(dois: Optional[list[str]] = typer.Argument(None, help="DOIs (bare, doi: or https://doi.org/ forms)"),
              file: Optional[str] = typer.Option(None, "--file", help="text file with one DOI per line"),
              refresh: bool = typer.Option(False, "--refresh", help="wait for the worker to process the events, then run dbt so search/statistics show them"),
              wait_seconds: int = typer.Option(120, "--wait-seconds", help="with --refresh: how long to wait for the worker")):
    """Targeted ingestion of specific Crossref records, then announce them on Kafka (streaming path).

    Same path as every other record: immutable raw file -> ODS (+manifest) -> outbox -> paper.raw.events -> worker -> DWD.
    Does not touch the incremental-sync watermark."""
    from pathlib import Path

    from paperforge.events.publisher import publish_outbox
    items = list(dois or [])
    if file:
        items += [ln.strip() for ln in Path(file).read_text().splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not items:
        raise typer.BadParameter("give at least one DOI or --file")
    s = get_settings()
    conn = CrossrefApiConnector(s.crossref_api_base, rate_per_sec=s.crossref_rate_per_sec)
    try:
        units = conn.plan_dois(items)
    except ValueError as e:
        raise typer.BadParameter(str(e))
    storage = LocalStorage(s.data_dir)
    runner = IngestionRunner(conn, storage, concurrency=s.crossref_concurrency)
    bid = runner.start("targeted", units, {"dois": [u.params["doi"] for u in units]}, None)
    res = runner.run(bid)
    pub = publish_outbox(_bus(), s.kafka_raw_topic, dois=[u.params["doi"] for u in units])   # only rows that exist in ODS
    typer.echo(f"published to {s.kafka_raw_topic}: {pub}")
    cur().set(published=pub["published"], publish_failed=pub["failed"])
    if res.status != "completed":
        with connect() as c:
            typer.echo(f"errors: {c.execute('SELECT error FROM ods.ingestion_batch WHERE batch_id=%s', (bid,)).fetchone()[0]}")
    ok = res.status == "completed" and not pub["failed"]
    stale = False
    if refresh and res.rows_inserted + res.rows_updated:
        ok = _wait_worker(s, wait_seconds) and ok
        if ok or _worker_done(s):          # events are in DWD even if another DOI of this batch failed
            import subprocess
            import sys
            typer.echo("refreshing dbt models ...")
            r = subprocess.run([sys.executable, "-m", "paperforge.cli", "dbt-build"], capture_output=True, text=True)
            typer.echo(r.stdout.strip().splitlines()[-1] if r.stdout.strip() else "dbt produced no output")
            if r.returncode:
                typer.echo(r.stderr[-1500:], err=True)
                ok = False
        else:
            typer.echo("the worker has not caught up yet: skipping dbt (is the worker running? `make status`)", err=True)
            stale = True
    elif res.rows_inserted + res.rows_updated:
        stale = True
    if stale:
        typer.echo("note: DWD is updated by the worker within seconds, but search and statistics (ads.*, dws.*) only change after "
                   "`make dbt` (or rerun with --refresh).")
    _report(res, storage)
    if not ok:
        raise typer.Exit(1)


def _worker_done(s) -> bool:
    """The worker commits offsets only after the DB commit, so zero lag means everything announced so far is processed (or in the DLQ)."""
    return sum(end - max(c, 0) for c, end in _bus().lag(s.kafka_group, s.kafka_raw_topic).values()) == 0


def _wait_worker(s, seconds: int) -> bool:
    import time
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if _worker_done(s):
            return True
        time.sleep(1)
    return False


def _watermark(source: str) -> date:
    with connect() as c:
        r = c.execute("SELECT window_until FROM meta.sync_checkpoint WHERE source=%s AND batch_id='' AND unit_key='watermark'", (source,)).fetchone()
    if not r:
        raise typer.BadParameter("no watermark yet: pass --since for the first sync")
    return r[0]


def _advance_watermark(source: str, batch_id: str):
    with connect() as c:
        until = c.execute("SELECT max(window_until) FROM meta.sync_checkpoint WHERE batch_id=%s", (batch_id,)).fetchone()[0]
        c.execute("""INSERT INTO meta.sync_checkpoint(source,batch_id,unit_key,status,window_until)
                     VALUES (%s,'','watermark','done',%s)
                     ON CONFLICT (source,batch_id,unit_key) DO UPDATE
                     SET window_until=GREATEST(meta.sync_checkpoint.window_until, EXCLUDED.window_until), updated_at=now()""",
                  (source, until))


@app.command("stats")
def stats(batch: Optional[str] = None):
    """HTTP request stats (status codes, latency, failure rate) and batch summary."""
    with connect() as c:
        w, a = ("WHERE batch_id=%s", (batch,)) if batch else ("", ())
        for r in c.execute(f"SELECT coalesce(status_code::text,'transport-error'), count(*) FROM ods.ingestion_request {w} GROUP BY 1 ORDER BY 1", a):
            typer.echo(f"status {r[0]}: {r[1]}")
        r = c.execute(f"""SELECT count(*), count(*) FILTER (WHERE status_code IS NULL OR status_code>=400),
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms)
                          FROM ods.ingestion_request {w}""", a).fetchone()
        typer.echo(f"requests={r[0]} failed={r[1]} p50_ms={r[2]} p95_ms={r[3]}")
        for r in c.execute("SELECT batch_id, status, rows_seen, rows_inserted, rows_unchanged, rows_skipped FROM ods.ingestion_batch ORDER BY started_at DESC LIMIT 5"):
            typer.echo(r)


@app.command("transform")
@tracked_cmd("transform", lineage="transform")
def transform_cmd(batch: Optional[str] = typer.Option(None, help="only this ingestion batch id"),
                  chunk: int = 1000,
                  rebuild: bool = typer.Option(False, help="TRUNCATE dwd.* first (use after normalizer rule changes; DWD is derived from raw)")):
    """Batch Mode: raw JSONL -> normalize/validate -> DWD (same engine as streaming)."""
    res = _transform(batch, chunk, rebuild)
    typer.echo(res)
    cur().set(rows_in=res.received, rows_out=res.inserted + res.updated + res.late, rows_failed=res.quarantined,
              inserted=res.inserted, updated=res.updated, late=res.late, refreshed=res.refreshed, unchanged=res.unchanged,
              warnings=res.warnings)


def _transform(batch: Optional[str], chunk: int, rebuild: bool = False):
    from paperforge.processing.batch import run_batch
    from paperforge.processing.engine import PolarsEngine
    from paperforge.repositories.pg_paper_repository import PgPaperRepository
    s = get_settings()
    repo = PgPaperRepository()
    if rebuild:
        repo.conn.execute("TRUNCATE dwd.paper, dwd.paper_identity_event CASCADE")
        repo.conn.execute("UPDATE meta.quarantine_record SET status='discarded' WHERE status='open'")  # superseded by this rebuild
        typer.echo("dwd truncated; rebuilding from raw")
    try:
        return run_batch(PolarsEngine(repo), LocalStorage(s.data_dir), batch_id=batch, chunk_size=chunk,
                         progress=lambda fid, t: typer.echo(f"file {fid}: {t}"))
    finally:
        repo.close()


events_app = typer.Typer(help="Kafka: topics, publishing, replay, status")
app.add_typer(events_app, name="events")


def _bus():
    from paperforge.events.kafka_bus import KafkaEventBus
    return KafkaEventBus(get_settings().kafka_bootstrap)


@events_app.command("init-topics")
def events_init_topics():
    s = get_settings()
    _bus().ensure_topics({s.kafka_raw_topic: s.kafka_partitions, s.kafka_dlq_topic: 1})
    typer.echo(f"topics ready: {s.kafka_raw_topic} ({s.kafka_partitions} partitions), {s.kafka_dlq_topic}")


@events_app.command("publish")
@tracked_cmd("publish", lineage="publish")
def events_publish(limit: Optional[int] = None):
    """Outbox: announce ODS rows with published_at IS NULL (marked only after broker ack)."""
    from paperforge.events.publisher import publish_outbox
    s = get_settings()
    res = publish_outbox(_bus(), s.kafka_raw_topic, limit=limit)
    typer.echo(res)
    cur().set(rows_in=res["published"] + res["failed"], rows_out=res["published"], rows_failed=res["failed"], data_version=res["trace_id"])
    if res["failed"]:
        raise typer.Exit(1)


@events_app.command("replay")
@tracked_cmd("replay_events", lineage="replay_events")
def events_replay(batch: Optional[str] = typer.Option(None, help="raw ingestion batch id (default: all raw files)")):
    """Re-publish immutable raw files as events (history replay)."""
    from paperforge.events.publisher import publish_raw_files
    s = get_settings()
    res = publish_raw_files(_bus(), s.kafka_raw_topic, LocalStorage(s.data_dir), batch_id=batch)
    typer.echo(res)
    cur().set(rows_in=res["published"] + res["failed"], rows_out=res["published"], rows_failed=res["failed"], data_version=res["trace_id"])
    if res["failed"]:
        raise typer.Exit(1)


@events_app.command("replay-dlq")
@tracked_cmd("replay_dlq", lineage="replay_dlq")
def events_replay_dlq(group: str = "paperforge-dlq-replay"):
    from paperforge.workers.consumer import replay_dlq
    s = get_settings()
    out = replay_dlq(_bus(), s.kafka_dlq_topic, s.kafka_raw_topic, group)
    typer.echo(out)
    cur().set(rows_in=out["replayed"] + out["unreplayable"], rows_out=out["replayed"], rows_failed=out["unreplayable"])


@events_app.command("status")
def events_status(group: Optional[str] = None):
    """Consumer lag per partition + end-to-end gap. A broker ack is NOT processing success: the gap is measured in PostgreSQL."""
    s = get_settings()
    bus = _bus()
    group = group or s.kafka_group
    total = 0
    for p, (committed, end) in sorted(bus.lag(group, s.kafka_raw_topic).items()):
        lag = end - max(committed, 0)
        total += lag
        typer.echo(f"partition {p}: committed={committed} end={end} lag={lag}")
    with connect() as c:
        unpub = c.execute("SELECT count(*) FROM ods.crossref_works WHERE published_at IS NULL").fetchone()[0]
        gap = c.execute("""SELECT count(*) FROM ods.crossref_works w WHERE NOT EXISTS (SELECT 1 FROM dwd.paper p WHERE p.primary_doi=w.doi_norm)
                           AND NOT EXISTS (SELECT 1 FROM meta.quarantine_record q WHERE q.status='open' AND q.source_id=w.doi_norm)""").fetchone()[0]
    typer.echo(f"group={group} total_lag={total} ods_unpublished={unpub} ods_not_in_dwd_or_quarantine={gap}")


worker_app = typer.Typer(help="Long-running Kafka consumer (Streaming Mode)")
app.add_typer(worker_app, name="worker")


@worker_app.command("run")
@tracked_cmd("stream_worker", lineage="stream_worker")
def worker_run(group: Optional[str] = None, idle_exit: Optional[float] = typer.Option(None, help="exit after N idle seconds (demo/tests)"),
               batch_size: Optional[int] = None):
    import signal
    from paperforge.processing.engine import PolarsEngine
    from paperforge.repositories.pg_paper_repository import PgPaperRepository
    from paperforge.workers.consumer import StreamWorker
    s = get_settings()
    repo = PgPaperRepository()
    stop = {"v": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("v", True))
    w = StreamWorker(_bus(), PolarsEngine(repo), group or s.kafka_group, s.kafka_raw_topic, s.kafka_dlq_topic,
                     batch_size or s.worker_batch_size, s.worker_batch_timeout_s, s.worker_max_retries, on_reconnect=repo.reconnect)
    typer.echo(f"worker started: group={w.group} topic={s.kafka_raw_topic}")
    try:
        st = w.run(lambda: stop["v"], idle_exit)
    finally:
        repo.close()
    typer.echo(st)
    cur().set(rows_in=st.messages, rows_out=st.result.inserted + st.result.updated + st.result.late, rows_failed=st.dlq + st.result.quarantined,
              batches=st.batches, unchanged=st.result.unchanged)


quality_app = typer.Typer(help="Data quality: rule runs, daily report, quarantine replay")
app.add_typer(quality_app, name="quality")


@quality_app.command("run")
@tracked_cmd("quality_run", lineage="quality_run")
def quality_run():
    """Execute all dataset-level rules (Pandera + SQL + ingest-rule roll-ups), record results. Exit 1 if an error rule fails."""
    from paperforge.quality.runner import has_blocking_failure, run_all
    with connect() as c:
        run_id, rows = run_all(c)
    for r in rows:
        if r["severity"] != "metric":
            typer.echo(f"{'PASS' if r['passed'] else ('FAIL' if r['severity']=='error' else 'WARN'):4} {r['severity']:7} {r['rule_id']:34} failed={r['failed']}/{r['total']}")
    typer.echo(f"run_id={run_id} rules={len(rows)}")
    failing = [r["rule_id"] for r in rows if r["severity"] == "error" and r["passed"] is False]
    cur().set(rows_in=len(rows), rows_failed=len(failing), data_version=str(run_id), failing_error_rules=failing,
              warning_findings=sum(1 for r in rows if r["severity"] == "warning" and r["passed"] is False))
    if has_blocking_failure(rows):
        raise typer.Exit(1)


@quality_app.command("report")
@tracked_cmd("quality_report")
def quality_report():
    """Write data/reports/quality-<date>.md from the latest recorded run."""
    from paperforge.quality.report import write_report
    with connect() as c:
        typer.echo(write_report(c, get_settings().data_dir))


@quality_app.command("replay-quarantine")
@tracked_cmd("replay_quarantine", lineage="replay_quarantine")
def quality_replay(rule: Optional[str] = None, limit: Optional[int] = None):
    """Re-process open quarantine records (after a rule change or payload fix) through the shared engine."""
    from paperforge.processing.engine import PolarsEngine
    from paperforge.quality.replay import replay_quarantine
    from paperforge.repositories.pg_paper_repository import PgPaperRepository
    repo = PgPaperRepository()
    try:
        with connect() as c:
            out = replay_quarantine(c, PolarsEngine(repo), rule, limit)
            typer.echo(out)
            cur().set(rows_in=out["examined"], rows_out=out["replayed"], rows_failed=out["still_failing"])
    finally:
        repo.close()


@quality_app.command("load-dbt")
@tracked_cmd("load_dbt_results")
def quality_load_dbt(path: str = "dbt/target/run_results.json"):
    """Record dbt test results (run_results.json) in meta.quality_result."""
    from paperforge.quality.dbt_results import load_run_results
    with connect() as c:
        out = load_run_results(c, path)
        typer.echo(out)
        cur().set(rows_in=sum(out.values()), rows_out=out["pass"], rows_failed=out["fail"], warn=out["warn"])


@app.command("reconcile-dwd")
@tracked_cmd("reconcile_dwd")
def reconcile_dwd():
    """ODS (raw, latest) vs DWD: coverage, and re-normalised ODS payload hash must equal dwd.paper.content_hash."""
    from paperforge.processing.normalizer import normalize_crossref
    bad, checked = [], 0
    with connect() as c:
        ods = c.execute("SELECT count(*) FROM ods.crossref_works").fetchone()[0]
        dwd = c.execute("SELECT count(*) FROM dwd.paper").fetchone()[0]
        quar = c.execute("SELECT count(DISTINCT source_id) FROM meta.quarantine_record WHERE status='open'").fetchone()[0]
        if ods != dwd + quar:
            bad.append(f"ods {ods} != dwd {dwd} + open quarantine {quar}")
        for q, msg in [("SELECT count(*) FROM dwd.paper p WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_identifier i WHERE i.paper_id=p.paper_id AND i.scheme='doi' AND i.value=p.primary_doi AND i.valid_to IS NULL)", "papers without active DOI identifier"),
                       ("SELECT count(*) FROM dwd.paper p WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_version v WHERE v.paper_id=p.paper_id AND v.version_no=p.current_version AND v.content_hash=p.content_hash)", "papers whose current_version row is missing/inconsistent"),
                       ("SELECT count(*) FROM dwd.paper p WHERE author_count <> (SELECT count(*) FROM dwd.paper_author a WHERE a.paper_id=p.paper_id)", "author_count mismatch"),
                       ("SELECT count(*) FROM (SELECT paper_id FROM dwd.paper_version WHERE valid_to IS NULL GROUP BY 1 HAVING count(*)<>1) x", "papers without exactly one open-ended version")]:
            n = c.execute(q).fetchone()[0]
            if n:
                bad.append(f"{n} {msg}")
        with c.transaction(), c.cursor(name="ods_scan") as cur:
            cur.execute("SELECT w.doi_norm, w.payload, p.content_hash FROM ods.crossref_works w LEFT JOIN dwd.paper p ON p.primary_doi=w.doi_norm")
            for doi, payload, h in cur:
                if h is None:
                    continue  # quarantined: accounted for above
                checked += 1
                if normalize_crossref(payload).content_hash != h and len(bad) < 20:
                    bad.append(f"content mismatch {doi}")
    typer.echo(f"ods={ods} dwd={dwd} open_quarantine={quar} content_hash_checked={checked}")
    for b in bad:
        typer.echo("MISMATCH " + b)
    if bad:
        raise typer.Exit(1)
    typer.echo("reconcile-dwd OK")


@app.command("reconcile")
@tracked_cmd("reconcile_raw")
def reconcile():
    """Raw files vs manifest vs ODS vs Parquet (read via DuckDB). Exit 1 on any mismatch."""
    import duckdb
    s = get_settings()
    storage = LocalStorage(s.data_dir)
    bad = []
    with connect() as c:
        manifest = c.execute("SELECT path, sha256, record_count FROM ods.raw_file_manifest ORDER BY path").fetchall()
        for path, sha, _ in manifest:
            if not storage.exists(path) or storage.checksum(path) != sha:
                bad.append(f"checksum/missing: {path}")
        on_disk = set(storage.list("raw"))
        bad += [f"unregistered raw file: {p}" for p in sorted(on_disk - {m[0] for m in manifest})]
        raw_rows = sum(m[2] for m in manifest)
        ods_lineage = c.execute("SELECT count(*) FROM ods.crossref_works w JOIN ods.raw_file_manifest m ON m.file_id=w.raw_file_id").fetchone()[0]
        ods_total = c.execute("SELECT count(*) FROM ods.crossref_works").fetchone()[0]
        # every batch's file counts must equal its rows_seen - rows_skipped
        mism = c.execute("""SELECT b.batch_id FROM ods.ingestion_batch b JOIN LATERAL
              (SELECT coalesce(sum(record_count),0) n FROM ods.raw_file_manifest m WHERE m.batch_id=b.batch_id) f ON true
              WHERE b.status='completed' AND f.n <> b.rows_seen - b.rows_skipped""").fetchall()
        bad += [f"batch count mismatch: {m[0]}" for m in mism]
        dois = {r[0] for r in c.execute("SELECT doi_norm FROM ods.crossref_works")}
    pq = f"{s.data_dir}/parquet/**/*.parquet"
    pq_rows, pq_distinct = duckdb.sql(f"SELECT count(*), count(DISTINCT doi) FROM read_parquet('{pq}')").fetchall()[0]
    pq_dois = {r[0] for r in duckdb.sql(f"SELECT DISTINCT doi FROM read_parquet('{pq}')").fetchall()}
    if pq_rows != raw_rows:
        bad.append(f"parquet rows {pq_rows} != raw rows {raw_rows}")
    if pq_dois != dois:
        bad.append(f"parquet distinct DOIs ({len(pq_dois)}) != ODS DOIs ({len(dois)})")
    typer.echo(f"raw files={len(manifest)} raw rows={raw_rows} parquet rows={pq_rows} (distinct {pq_distinct}) "
               f"ods rows={ods_total} ods rows with file lineage={ods_lineage}")
    for b in bad:
        typer.echo("MISMATCH " + b)
    if bad or ods_lineage != ods_total:
        raise typer.Exit(1)
    typer.echo("reconcile OK")


@app.command("dbt-build")
@tracked_cmd("dbt_build")
def dbt_build(select: Optional[str] = None, full_refresh: bool = False):
    """Run `dbt build` (models + tests); records per-invocation counts. Fails the task if any model/test errors."""
    import json
    import subprocess
    import sys
    from pathlib import Path
    root = Path(get_settings().project_dir).resolve() / "dbt"
    target = Path(get_settings().dbt_target_path)
    target = target if target.is_absolute() else root / target
    cmd = [str(Path(sys.executable).parent / "dbt"), "build", "--profiles-dir", str(root), "--project-dir", str(root),
           "--target-path", str(target)]
    cmd += (["--select", select] if select else []) + (["--full-refresh"] if full_refresh else [])
    r = subprocess.run(cmd, cwd=root, capture_output=True, text=True)
    typer.echo(r.stdout[-4000:])
    rr = json.loads((target / "run_results.json").read_text())
    counts: dict[str, int] = {}
    for x in rr["results"]:
        counts[x["status"]] = counts.get(x["status"], 0) + 1
    models = [x for x in rr["results"] if x["unique_id"].startswith("model.")]
    rows = sum((x.get("adapter_response") or {}).get("rows_affected") or 0 for x in models)
    cur().set(rows_in=len(rr["results"]), rows_out=rows, rows_failed=counts.get("error", 0) + counts.get("fail", 0),
              data_version=rr["metadata"].get("invocation_id"), statuses=counts, models=len(models),
              elapsed_s=round(rr.get("elapsed_time", 0), 2))
    if r.returncode:
        typer.echo(r.stderr[-2000:], err=True)
        raise typer.Exit(r.returncode)


lineage_app = typer.Typer(help="Dataset-level lineage and catalog")
app.add_typer(lineage_app, name="lineage")


@lineage_app.command("sync-dbt")
@tracked_cmd("lineage_sync_dbt")
def lineage_sync_dbt(manifest: Optional[str] = None):
    """Import dbt model dependencies (manifest.json) as lineage edges and catalog entries."""
    from pathlib import Path
    from paperforge.lineage.catalog import merge_column_descriptions
    from paperforge.lineage.dbt_manifest import parse_manifest, sync_dbt
    from paperforge.lineage.pg_emitter import PgLineageEmitter
    tgt = Path(get_settings().dbt_target_path)
    path = manifest or str(tgt / "manifest.json" if tgt.is_absolute() else Path(get_settings().project_dir) / "dbt" / tgt / "manifest.json")
    with connect() as c:
        out = sync_dbt(c, PgLineageEmitter(c), path)
        merge_column_descriptions(c, parse_manifest(path)[1])
    typer.echo(out)
    cur().set(rows_in=out["edges"], rows_out=out["models"], removed_stale_edges=out["removed_stale_edges"])


@lineage_app.command("show")
def lineage_show(dataset: str, direction: str = typer.Option("up", help="up (what feeds it) | down (what it feeds)")):
    from paperforge.lineage.query import trace
    with connect() as c:
        for e in trace(c, dataset, direction):
            typer.echo(f"{'  ' * (e['depth'] - 1)}{e['upstream']} -> {e['downstream']}   [{e['source']}:{e['task_id']}]")


catalog_app = typer.Typer(help="Data catalog")
app.add_typer(catalog_app, name="catalog")


@catalog_app.command("refresh")
@tracked_cmd("catalog_refresh")
def catalog_refresh():
    """Register static datasets and refresh column types, row counts and newest-data timestamps."""
    from paperforge.lineage.catalog import refresh
    from paperforge.lineage.datasets import register_static
    with connect() as c:
        register_static(c)
        out = refresh(c)
    typer.echo(out)
    cur().set(rows_out=out["refreshed"], not_built=out["not_built"])


pipeline_app = typer.Typer(help="Pipeline run bookkeeping")
app.add_typer(pipeline_app, name="pipeline")


@pipeline_app.command("reap")
def pipeline_reap(older_than_minutes: int = 120):
    """Mark task/pipeline runs that never recorded completion (killed processes) as failed."""
    from paperforge.runs import reap_stale
    typer.echo(reap_stale(older_than_minutes))


@pipeline_app.command("finish")
def pipeline_finish(status: Optional[str] = typer.Option(None, help="success|failed; default derived from task runs"),
                    fail_if_failed: bool = typer.Option(False, help="exit 1 when the run failed (so the Airflow DAG run fails too)")):
    """Close the Airflow-orchestrated run (needs PF_DAG_ID / PF_RUN_ID)."""
    from paperforge.runs import finish_run
    st = finish_run(status)
    typer.echo(st)
    if fail_if_failed and st == "failed":
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
