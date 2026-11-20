"""Catalog refresh: column types from information_schema, exact row counts and newest data timestamps."""
import psycopg
from psycopg.types.json import Jsonb

# dataset -> column holding the row's latest time (for "updated at")
TIME_COLUMN = {"ods.crossref_works": "ingested_at", "ods.ingestion_batch": "started_at", "ods.raw_file_manifest": "created_at",
               "ods.ingestion_request": "at", "dwd.paper": "updated_at", "dwd.paper_version": "created_at",
               "dwd.paper_identifier": "valid_from", "meta.quarantine_record": "created_at", "meta.quality_result": "run_at",
               "meta.pipeline_run": "started_at", "meta.task_run": "started_at", "meta.sync_checkpoint": "updated_at",
               "meta.lineage_edge": "last_seen_at", "dwd.paper_identity_event": "recorded_at", "meta.dataset": "refreshed_at"}


def refresh(conn: psycopg.Connection) -> dict:
    done = skipped = 0
    rows = conn.execute("SELECT dataset_id FROM meta.dataset WHERE kind IN ('table','view') ORDER BY dataset_id").fetchall()
    for (ds,) in rows:
        schema, name = ds.split(".", 1)
        cols = conn.execute("""SELECT column_name, data_type, is_nullable FROM information_schema.columns
                               WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position""", (schema, name)).fetchall()
        if not cols:
            skipped += 1                       # registered (e.g. via lineage) but not built yet
            continue
        existing = conn.execute("SELECT columns FROM meta.dataset WHERE dataset_id=%s", (ds,)).fetchone()[0] or []
        desc = {x["name"]: x.get("description") for x in existing}
        colspec = [{"name": c, "type": t, "nullable": n == "YES", "description": desc.get(c)} for c, t, n in cols]
        tcol = TIME_COLUMN.get(ds) or next((c for c in ("updated_at", "_loaded_at", "created_at") if c in {x[0] for x in cols}), None)
        sel = f'count(*), max("{tcol}")::timestamptz' if tcol else "count(*), NULL"
        n, last = conn.execute(f'SELECT {sel} FROM "{schema}"."{name}"').fetchone()
        conn.execute("UPDATE meta.dataset SET columns=%s, row_count=%s, last_data_at=%s, refreshed_at=now() WHERE dataset_id=%s",
                     (Jsonb(colspec), n, last, ds))
        done += 1
    return {"refreshed": done, "not_built": skipped}


def merge_column_descriptions(conn: psycopg.Connection, info: dict[str, dict]) -> None:
    """Carry dbt column descriptions into the catalog (types/counts come from refresh())."""
    for ds, i in info.items():
        if i["columns"]:
            conn.execute("UPDATE meta.dataset SET columns=%s WHERE dataset_id=%s",
                         (Jsonb([{"name": c["name"], "description": c["description"]} for c in i["columns"]]), ds))
