# Final acceptance (requirement §20)

State of the real warehouse when this was written (2026-11-19): ODS 101,000 DOIs · DWD 101,000 papers / 101,000 versions · 431,141 authors · 2,659,436 references ·
14,615 journals · 3,615 publishers · 136 immutable raw files holding 116,200 source lines (re-ingestions are kept) · 40 catalogued datasets · 59 lineage edges · 91 registered quality rules · open quarantine 0.
Test suite: **101/101 passed** (`docs/test_report.md`).

| # | Criterion | Status | Evidence / caveat |
|---|---|---|---|
| 1 | ~100,000 real Crossref papers ingested | **Met** | 100,000 sampled from the real Crossref dump (offline Parquet, 3 of 73 parts) + 1,000 from the **live API** → 101,000 distinct DOIs. Stage 1 report. *Caveat:* the 100,000 came from the offline dump (flattened columns), the API was used for the 1,000-record smoke test and incremental windows. |
| 2 | All raw records traceable | **Met** | every `ods` row and every `dwd.paper_version` stores `raw_file_id` + `raw_line`; sha256 per file; `reconcile` and rule `r_version_raw_lineage` pass; UI shows raw vs standardized with file/line. |
| 3 | Re-processing adds no duplicate entities | **Met** | `UNIQUE(primary_doi)`; full re-run of 112,300 records → 0 inserted; 4-thread concurrency test; streaming rebuild of 101,000 events from Kafka reproduced the same MD5. |
| 4 | Metadata updates keep history | **Met** | `paper_version` timeline with `valid_from/valid_to`; API `/papers/{id}/versions` + console tab; late/out-of-order tests. *Caveat:* real data contains no organic multi-version papers yet (single day of ingestion); versioning is proven on fixtures and replayed history. |
| 5 | Ingestion checkpoint recovery | **Met** | per-page transaction + checkpoint; interrupted-run tests incl. orphan-file case. *Not verified:* Crossref's real behaviour for an expired cursor (mocked, treated as HTTP 400). |
| 6 | Incremental sync and history replay | **Met (with caveat)** | `sync` by index-date windows + watermark (truncated runs never advance it); replay of raw files / batches / DLQ / quarantine. Live API verified on windows 2026-11-17 and overlap re-fetch. *Caveat:* no multi-day unattended scheduled run was observed. |
| 7 | Unified batch and streaming logic | **Met** | single `PolarsEngine.process()`; real-data comparison: DWD rebuilt purely through Kafka = same MD5 as batch result; fixture tests incl. out-of-order, late, duplicate. |
| 8 | Complete quality statistics | **Met** | 35 dataset rules (Pandera + SQL + ingest roll-up, 6 dimensions) + 56 dbt tests, execution records in `meta.quality_result`, daily report, console page. Result: 0 Error failures; 2 warnings (23 papers without title, 1 invalid date). |
| 9 | dbt modelling + dependency display | **Met** | 17 models (table / view / incremental), 56 tests (73 nodes pass), docs site (`dbt docs generate --static`), `docs/dbt-dag.md`; incremental vs full-refresh equality test (with a mutation check). |
| 10 | End-to-end lineage | **Met (as built)** | `external.crossref_* → raw → ods → kafka → dwd → stg → int → dim/dws → ads`, queryable by API/CLI/UI. *Differs from the sketch* "Crossref → Kafka → ODS": ODS is written first, then announced to Kafka (outbox). Dataset-level only. |
| 11 | Console driven by real data | **Met** | six pages; every figure from `/api/v1`; browser-verified (`frontend/ui-check.mjs`), no mock data in the repo. |
| 12 | Docker Compose deployment | **Met** | `postgres`, `kafka` (KRaft), `worker`, `api`, `frontend` + optional `airflow` profile; `make up`, `make demo` run from the repo (demo exit 0). *Caveat:* verified on one macOS/Docker Desktop machine only. |
| 13 | Architecture, ER, data-flow diagrams, data dictionary, README | **Met** | `docs/architecture.md`, `docs/er.md` (generated from PostgreSQL constraints), `docs/data_dictionary.md` (generated from the catalog), `README.md`. |
| 14 | Actual test report | **Met** | `docs/test_report.md`, generated from a real run (101/101). |

## Requirement-by-requirement notes where the result deliberately differs from the text
- **Raw JSON.** After your feedback the raw layer stores *source-native records* (offline dump rows / API items), not verbatim API JSON; both have the same Crossref field shapes. Raw hashes therefore differ between the two sources for the same paper; the content hash used for versioning ignores volatile fields.
- **Domain coverage** is a common-sense heuristic (DOI prefixes + journal-name keywords + quotas 10/28/30/20/12 %), not a subject classification. Offline data has no `subject` field, so there is no post-hoc validation of the domains.
- **Dimensions** are identifier-based only. `dim.journal` merges ISSN-co-listed identifiers one hop (not a transitive closure); papers without ISSN have no journal key.
- **Entity merge/split, OpenAlex/DataCite/PubMed** are represented by tables (`paper_identity_event`, `paper_identifier`) and the `SourceConnector` interface only; no algorithm or second connector was built.
- **Airflow** DAGs were executed with `airflow dags test` (in-process, real tasks) and a deliberate failure drill; no full `@daily` scheduled day, UI backfill or catch-up was exercised. Auth is "all admins", bound to 127.0.0.1.

## Known limitations
Single node and single Kafka broker (replication 1); at-least-once with idempotent writes (no Kafka transactions); throughput ≈ 1,400 events/s on one worker (reference only);
exact `count(*)` in the catalog will not scale to hundreds of millions of rows; ISSN checked by format only; equal-timestamp conflicting versions are resolved by arrival order;
incremental dbt models rely on a 1-hour look-back window; Crossref API latency through the local proxy was ~25–35 s per 1,000-row page (not representative).
