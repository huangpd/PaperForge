"""Dataset registry: static descriptions of the non-dbt datasets (dbt models are registered from manifest.json)."""
import psycopg
from psycopg.types.json import Jsonb

LAYERS = ("source", "raw", "kafka", "ods", "dwd", "dim", "dws", "ads", "meta", "stg", "int")

# dataset_id -> (kind, description, grain, primary_key, unique_constraints, update_strategy)
STATIC = {
    "external.crossref_api": ("external", "Crossref REST API /works (journal-article), incremental by index-date + cursor", "one work", "DOI", "-", "pulled by `sync`"),
    "external.crossref_parquet": ("external", "Offline Crossref dump (flattened parquet, read-only) used for bootstrap sampling", "one work", "DOI", "-", "never written"),
    "external.arxiv_snapshot": ("external", "arXiv metadata snapshot (Kaggle, one JSON per line, zip), read-only; scoped by category on ingestion", "one arXiv submission", "arXiv id", "-", "never written"),
    "raw.arxiv_jsonl": ("files", "Immutable JSONL files data/raw/source=arxiv/date=/batch_id=/ holding only the chosen category scope", "one source record per line", "(path, line)", "path unique", "append-only, never overwritten"),
    "raw.crossref_jsonl": ("files", "Immutable JSONL files data/raw/source=crossref/date=/batch_id=/ plus derived parquet", "one source record per line", "(path, line)", "path unique", "append-only, never overwritten"),
    "kafka.paper.raw.events": ("topic", "Raw paper events (key = normalized DOI), 3 partitions", "one event", "(partition, offset)", "event_id deterministic", "append-only, 14 day retention"),
    "kafka.paper.dlq": ("topic", "Events that could not be processed (original event + error context)", "one failed event", "(partition, offset)", "-", "append-only"),
    "ods.crossref_works": ("table", "Latest source-native record per DOI with lineage to raw file/line", "one DOI", "doi_norm", "doi_norm", "UPSERT when payload_hash changes and source_updated_at is not older"),
    "ods.arxiv_works": ("table", "Latest source-native arXiv record per arXiv id with lineage to raw file/line", "one arXiv id", "arxiv_id", "arxiv_id", "UPSERT when payload_hash changes and source_updated_at is not older"),
    "ods.ingestion_batch": ("table", "One row per ingestion run", "one batch", "batch_id", "batch_id", "status machine"),
    "ods.raw_file_manifest": ("table", "Index of immutable raw files with sha256 and row counts", "one file", "file_id", "path; (batch_id, unit_key, seq)", "append-only"),
    "ods.ingestion_request": ("table", "HTTP request log: status code, latency, attempt", "one request", "id", "-", "append-only"),
    "dwd.paper": ("table", "Current paper entity (internal UUIDv7 paper_id); paper_key = DOI, or arxiv:<id> for arXiv papers without a DOI", "one paper", "paper_id", "paper_key; primary_doi (when present)", "transactional UPSERT; version timeline protects against out-of-order events"),
    "dwd.paper_arxiv": ("table", "arXiv-specific data of a paper: categories, arXiv abstract, journal-ref, license, submission/update dates", "one arXiv submission", "arxiv_id", "arxiv_id", "UPSERT when content_hash changes; never writes paper versions for Crossref-owned papers"),
    "dwd.paper_identifier": ("table", "External identifiers of a paper with validity interval", "one identifier", "id", "(scheme,value,source) where valid_to is null", "append; close with valid_to"),
    "dwd.paper_author": ("table", "Authors of the current version", "one author of a paper", "(paper_id, ordinal)", "(paper_id, ordinal)", "replaced when the current version changes"),
    "dwd.paper_reference": ("table", "References of the current version", "one reference of a paper", "(paper_id, ordinal)", "(paper_id, ordinal)", "replaced when the current version changes"),
    "dwd.paper_version": ("table", "History: one row per distinct content on the source_updated_at timeline", "one version", "(paper_id, version_no)", "(paper_id, source_updated_at, content_hash)", "append-only; valid_to recomputed"),
    "dwd.paper_identity_event": ("table", "Identifier change / merge / split events (structure reserved)", "one event", "event_id", "event_id", "append-only"),
    "meta.quarantine_record": ("table", "Records rejected by Error rules, with raw payload, rule, version, status", "one rejected record per rule", "id", "open (source, source_id, rule_id, payload_hash)", "status open -> replayed|discarded"),
    "meta.quality_rule": ("table", "Quality rule catalog", "one rule version", "(rule_id, version)", "(rule_id, version)", "upsert on sync"),
    "meta.quality_result": ("table", "Quality rule execution records", "one rule execution", "id", "-", "append-only"),
    "meta.sync_checkpoint": ("table", "Ingestion window checkpoints and per-source watermark", "one (batch, unit) or watermark", "(source, batch_id, unit_key)", "(source, batch_id, unit_key)", "updated per page"),
    "meta.pipeline_run": ("table", "Pipeline executions", "one run", "run_id", "(dag_id, external_run_id)", "status machine"),
    "meta.task_run": ("table", "Task attempts with row counts, data version, duration", "one task attempt", "task_run_id", "-", "status machine"),
    "meta.lineage_edge": ("table", "Dataset-level lineage edges (dbt manifest + runtime registration)", "one edge per origin", "id", "(upstream, downstream, source)", "upsert / replace per origin"),
    "meta.dataset": ("table", "Data catalog", "one dataset", "dataset_id", "dataset_id", "refreshed by `catalog refresh`"),
}


def layer_of(dataset_id: str) -> str:
    head = dataset_id.split(".", 1)[0]
    return head if head in LAYERS else ("source" if head == "external" else "meta")


def ensure_dataset(conn: psycopg.Connection, dataset_id: str) -> None:
    spec = STATIC.get(dataset_id)
    kind, desc, grain, pk, uq, upd = spec if spec else ("table", None, None, None, None, None)
    conn.execute(
        """INSERT INTO meta.dataset(dataset_id, layer, kind, description, grain, primary_key, unique_constraints, update_strategy)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (dataset_id) DO UPDATE SET
             description=coalesce(EXCLUDED.description, meta.dataset.description), grain=coalesce(EXCLUDED.grain, meta.dataset.grain),
             primary_key=coalesce(EXCLUDED.primary_key, meta.dataset.primary_key),
             unique_constraints=coalesce(EXCLUDED.unique_constraints, meta.dataset.unique_constraints),
             update_strategy=coalesce(EXCLUDED.update_strategy, meta.dataset.update_strategy)""",
        (dataset_id, layer_of(dataset_id), kind, desc, grain, pk, uq, upd))


def register_static(conn: psycopg.Connection) -> int:
    for d in STATIC:
        ensure_dataset(conn, d)
    return len(STATIC)


# Runtime edges: which task moves data from where to where. Registered when the task actually runs.
RUNTIME_EDGES = {
    "ingest": [("external.crossref_parquet", "raw.crossref_jsonl"), ("raw.crossref_jsonl", "ods.crossref_works"),
               ("raw.crossref_jsonl", "ods.raw_file_manifest"), ("raw.crossref_jsonl", "ods.ingestion_batch")],
    "sync": [("external.crossref_api", "raw.crossref_jsonl"), ("external.crossref_api", "ods.ingestion_request"),
             ("raw.crossref_jsonl", "ods.crossref_works"), ("raw.crossref_jsonl", "ods.raw_file_manifest"),
             ("raw.crossref_jsonl", "ods.ingestion_batch")],
    "fetch_doi": [("external.crossref_api", "raw.crossref_jsonl"), ("external.crossref_api", "ods.ingestion_request"),
                  ("raw.crossref_jsonl", "ods.crossref_works"), ("raw.crossref_jsonl", "ods.raw_file_manifest"),
                  ("raw.crossref_jsonl", "ods.ingestion_batch"), ("ods.crossref_works", "kafka.paper.raw.events")],
    "ingest_arxiv": [("external.arxiv_snapshot", "raw.arxiv_jsonl"), ("raw.arxiv_jsonl", "ods.arxiv_works"),
                     ("raw.arxiv_jsonl", "ods.raw_file_manifest"), ("raw.arxiv_jsonl", "ods.ingestion_batch"),
                     ("raw.arxiv_jsonl", "dwd.paper"), ("raw.arxiv_jsonl", "dwd.paper_arxiv"), ("raw.arxiv_jsonl", "dwd.paper_version"),
                     ("raw.arxiv_jsonl", "dwd.paper_author"), ("raw.arxiv_jsonl", "dwd.paper_identifier"),
                     ("raw.arxiv_jsonl", "meta.quarantine_record")],
    "publish": [("ods.crossref_works", "kafka.paper.raw.events")],
    "replay_events": [("raw.crossref_jsonl", "kafka.paper.raw.events")],
    "transform": [("raw.crossref_jsonl", "dwd.paper"), ("raw.crossref_jsonl", "dwd.paper_version"),
                  ("raw.crossref_jsonl", "dwd.paper_author"), ("raw.crossref_jsonl", "dwd.paper_reference"),
                  ("raw.crossref_jsonl", "dwd.paper_identifier"), ("raw.crossref_jsonl", "meta.quarantine_record")],
    "stream_worker": [("kafka.paper.raw.events", "dwd.paper"), ("kafka.paper.raw.events", "dwd.paper_version"),
                      ("kafka.paper.raw.events", "dwd.paper_author"), ("kafka.paper.raw.events", "dwd.paper_reference"),
                      ("kafka.paper.raw.events", "dwd.paper_identifier"), ("kafka.paper.raw.events", "meta.quarantine_record"),
                      ("kafka.paper.raw.events", "kafka.paper.dlq")],
    "quality_run": [("dwd.paper", "meta.quality_result"), ("ods.crossref_works", "meta.quality_result"),
                    ("meta.quarantine_record", "meta.quality_result")],
    "replay_quarantine": [("meta.quarantine_record", "dwd.paper")],
    "replay_dlq": [("kafka.paper.dlq", "kafka.paper.raw.events")],
}

Json = Jsonb
