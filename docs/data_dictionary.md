# Data dictionary

Generated from `meta.dataset` (catalog) and `meta.lineage_edge` by `scripts/gen_docs.py`; row counts are from the last `catalog refresh`.

## Sources

### `external.crossref_api`

Crossref REST API /works (journal-article), incremental by index-date + cursor

- **Kind:** external
- **Grain:** one work
- **Primary key:** `DOI`
- **Unique constraints:** -
- **Update strategy:** pulled by `sync`
- **Upstream:** —
- **Rows:** —

### `external.crossref_parquet`

Offline Crossref dump (flattened parquet, read-only) used for bootstrap sampling

- **Kind:** external
- **Grain:** one work
- **Primary key:** `DOI`
- **Unique constraints:** -
- **Update strategy:** never written
- **Upstream:** —
- **Rows:** —

## Raw archive

### `raw.crossref_jsonl`

Immutable JSONL files data/raw/source=crossref/date=/batch_id=/ plus derived parquet

- **Kind:** files
- **Grain:** one source record per line
- **Primary key:** `(path, line)`
- **Unique constraints:** path unique
- **Update strategy:** append-only, never overwritten
- **Upstream:** `external.crossref_api`, `external.crossref_parquet`
- **Rows:** —

## Kafka topics

### `kafka.paper.dlq`

Events that could not be processed (original event + error context)

- **Kind:** topic
- **Grain:** one failed event
- **Primary key:** `(partition, offset)`
- **Unique constraints:** -
- **Update strategy:** append-only
- **Upstream:** `kafka.paper.raw.events`
- **Rows:** —

### `kafka.paper.raw.events`

Raw paper events (key = normalized DOI), 3 partitions

- **Kind:** topic
- **Grain:** one event
- **Primary key:** `(partition, offset)`
- **Unique constraints:** event_id deterministic
- **Update strategy:** append-only, 14 day retention
- **Upstream:** `kafka.paper.dlq`, `ods.crossref_works`, `raw.crossref_jsonl`
- **Rows:** —

## ODS — raw data layer

### `ods.crossref_works`

Latest source-native record per DOI with lineage to raw file/line

- **Kind:** table
- **Grain:** one DOI
- **Primary key:** `doi_norm`
- **Unique constraints:** doi_norm
- **Update strategy:** UPSERT when payload_hash changes and source_updated_at is not older
- **Upstream:** `raw.crossref_jsonl`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `doi_norm` | text | no |  |
| `source` | text | no |  |
| `source_id` | text | no |  |
| `payload` | jsonb | no |  |
| `payload_hash` | text | no |  |
| `source_updated_at` | timestamp with time zone | yes |  |
| `batch_id` | text | no |  |
| `raw_file_id` | bigint | no |  |
| `raw_line` | integer | no |  |
| `first_seen_at` | timestamp with time zone | no |  |
| `ingested_at` | timestamp with time zone | no |  |
| `published_at` | timestamp with time zone | yes |  |

### `ods.ingestion_batch`

One row per ingestion run

- **Kind:** table
- **Grain:** one batch
- **Primary key:** `batch_id`
- **Unique constraints:** batch_id
- **Update strategy:** status machine
- **Upstream:** `raw.crossref_jsonl`
- **Rows:** 22

| column | type | null | description |
|---|---|---|---|
| `batch_id` | text | no |  |
| `source` | text | no |  |
| `mode` | text | no |  |
| `status` | text | no |  |
| `params` | jsonb | no |  |
| `rows_seen` | bigint | no |  |
| `rows_inserted` | bigint | no |  |
| `rows_updated` | bigint | no |  |
| `rows_unchanged` | bigint | no |  |
| `rows_skipped` | bigint | no |  |
| `started_at` | timestamp with time zone | no |  |
| `finished_at` | timestamp with time zone | yes |  |
| `error` | text | yes |  |

### `ods.ingestion_request`

HTTP request log: status code, latency, attempt

- **Kind:** table
- **Grain:** one request
- **Primary key:** `id`
- **Unique constraints:** -
- **Update strategy:** append-only
- **Upstream:** `external.crossref_api`
- **Rows:** 9

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | no |  |
| `batch_id` | text | no |  |
| `unit_key` | text | no |  |
| `status_code` | integer | yes |  |
| `elapsed_ms` | double precision | no |  |
| `attempt` | integer | no |  |
| `error` | text | yes |  |
| `at` | timestamp with time zone | no |  |

### `ods.raw_file_manifest`

Index of immutable raw files with sha256 and row counts

- **Kind:** table
- **Grain:** one file
- **Primary key:** `file_id`
- **Unique constraints:** path; (batch_id, unit_key, seq)
- **Update strategy:** append-only
- **Upstream:** `raw.crossref_jsonl`
- **Rows:** 136

| column | type | null | description |
|---|---|---|---|
| `file_id` | bigint | no |  |
| `batch_id` | text | no |  |
| `unit_key` | text | no |  |
| `seq` | integer | no |  |
| `path` | text | no |  |
| `record_count` | integer | no |  |
| `size_bytes` | bigint | no |  |
| `sha256` | text | no |  |
| `created_at` | timestamp with time zone | no |  |

## DWD — standardized detail

### `dwd.paper`

Current paper entity (internal UUIDv7 paper_id)

- **Kind:** table
- **Grain:** one paper
- **Primary key:** `paper_id`
- **Unique constraints:** primary_doi
- **Update strategy:** transactional UPSERT; version timeline protects against out-of-order events
- **Upstream:** `kafka.paper.raw.events`, `meta.quarantine_record`, `raw.crossref_jsonl`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | no |  |
| `primary_doi` | text | no |  |
| `title` | text | yes |  |
| `abstract` | text | yes |  |
| `container_title` | text | yes |  |
| `publisher` | text | yes |  |
| `member` | text | yes |  |
| `type` | text | yes |  |
| `language` | text | yes |  |
| `volume` | text | yes |  |
| `issue` | text | yes |  |
| `page` | text | yes |  |
| `issns` | ARRAY | no |  |
| `issued_date` | date | yes |  |
| `issued_year` | integer | yes |  |
| `issued_precision` | text | yes |  |
| `reference_count` | integer | yes |  |
| `cited_by_count` | integer | yes |  |
| `author_count` | integer | no |  |
| `current_version` | integer | no |  |
| `content_hash` | text | no |  |
| `source_updated_at` | timestamp with time zone | no |  |
| `created_at` | timestamp with time zone | no |  |
| `updated_at` | timestamp with time zone | no |  |

### `dwd.paper_author`

Authors of the current version

- **Kind:** table
- **Grain:** one author of a paper
- **Primary key:** `(paper_id, ordinal)`
- **Unique constraints:** (paper_id, ordinal)
- **Update strategy:** replaced when the current version changes
- **Upstream:** `kafka.paper.raw.events`, `raw.crossref_jsonl`
- **Rows:** 431,141

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | no |  |
| `ordinal` | integer | no |  |
| `given` | text | yes |  |
| `family` | text | yes |  |
| `name` | text | yes |  |
| `orcid` | text | yes |  |
| `sequence` | text | yes |  |
| `affiliations` | ARRAY | no |  |

### `dwd.paper_identifier`

External identifiers of a paper with validity interval

- **Kind:** table
- **Grain:** one identifier
- **Primary key:** `id`
- **Unique constraints:** (scheme,value,source) where valid_to is null
- **Update strategy:** append; close with valid_to
- **Upstream:** `kafka.paper.raw.events`, `raw.crossref_jsonl`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | no |  |
| `paper_id` | uuid | no |  |
| `scheme` | text | no |  |
| `value` | text | no |  |
| `source` | text | no |  |
| `valid_from` | timestamp with time zone | no |  |
| `valid_to` | timestamp with time zone | yes |  |
| `status` | text | no |  |

### `dwd.paper_identity_event`

Identifier change / merge / split events (structure reserved)

- **Kind:** table
- **Grain:** one event
- **Primary key:** `event_id`
- **Unique constraints:** event_id
- **Update strategy:** append-only
- **Upstream:** —
- **Rows:** 0

| column | type | null | description |
|---|---|---|---|
| `event_id` | uuid | no |  |
| `event_type` | text | no |  |
| `from_paper_id` | uuid | yes |  |
| `to_paper_id` | uuid | yes |  |
| `payload` | jsonb | no |  |
| `occurred_at` | timestamp with time zone | no |  |
| `recorded_at` | timestamp with time zone | no |  |

### `dwd.paper_reference`

References of the current version

- **Kind:** table
- **Grain:** one reference of a paper
- **Primary key:** `(paper_id, ordinal)`
- **Unique constraints:** (paper_id, ordinal)
- **Update strategy:** replaced when the current version changes
- **Upstream:** `kafka.paper.raw.events`, `raw.crossref_jsonl`
- **Rows:** 2,659,436

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | no |  |
| `ordinal` | integer | no |  |
| `ref_key` | text | yes |  |
| `ref_doi` | text | yes |  |
| `unstructured` | text | yes |  |
| `article_title` | text | yes |  |
| `journal_title` | text | yes |  |
| `ref_year` | integer | yes |  |

### `dwd.paper_version`

History: one row per distinct content on the source_updated_at timeline

- **Kind:** table
- **Grain:** one version
- **Primary key:** `(paper_id, version_no)`
- **Unique constraints:** (paper_id, source_updated_at, content_hash)
- **Update strategy:** append-only; valid_to recomputed
- **Upstream:** `kafka.paper.raw.events`, `raw.crossref_jsonl`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | no |  |
| `version_no` | integer | no |  |
| `content_hash` | text | no |  |
| `snapshot` | jsonb | no |  |
| `source_updated_at` | timestamp with time zone | no |  |
| `valid_from` | timestamp with time zone | no |  |
| `valid_to` | timestamp with time zone | yes |  |
| `raw_file_id` | bigint | yes |  |
| `raw_line` | integer | yes |  |
| `normalizer_version` | integer | no |  |
| `quality_flags` | jsonb | no |  |
| `created_at` | timestamp with time zone | no |  |

## dbt staging (views)

### `stg.stg_paper`

1:1 view of dwd.paper with completeness flags. Responsibility: rename + flags only.

- **Kind:** view (view)
- **Grain:** one paper
- **Primary key:** `paper_id`
- **Unique constraints:** paper_id; doi
- **Update strategy:** view: always reflects dwd.paper
- **Upstream:** `dwd.paper`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | yes |  |
| `doi` | text | yes |  |
| `title` | text | yes |  |
| `abstract` | text | yes |  |
| `container_title` | text | yes |  |
| `publisher` | text | yes |  |
| `member` | text | yes |  |
| `type` | text | yes |  |
| `language` | text | yes |  |
| `issns` | ARRAY | yes |  |
| `issued_date` | date | yes |  |
| `issued_year` | integer | yes |  |
| `issued_precision` | text | yes |  |
| `reference_count` | integer | yes |  |
| `cited_by_count` | integer | yes |  |
| `author_count` | integer | yes |  |
| `current_version` | integer | yes |  |
| `source_updated_at` | timestamp with time zone | yes |  |
| `created_at` | timestamp with time zone | yes |  |
| `updated_at` | timestamp with time zone | yes |  |
| `has_title` | boolean | yes |  |
| `has_abstract` | boolean | yes |  |
| `has_authors` | boolean | yes |  |
| `has_references` | boolean | yes |  |
| `has_issn` | boolean | yes |  |
| `has_issued_date` | boolean | yes |  |
| `has_member` | boolean | yes |  |
| `has_container_title` | boolean | yes |  |

### `stg.stg_paper_issn`

One row per (paper, ISSN).

- **Kind:** view (view)
- **Grain:** one (paper, ISSN)
- **Primary key:** `(paper_id, issn)`
- **Unique constraints:** (paper_id, issn)
- **Update strategy:** view
- **Upstream:** `dwd.paper`
- **Rows:** 151,057

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | yes |  |
| `issn` | text | yes |  |

### `stg.stg_paper_version`

1:1 view of dwd.paper_version metadata (no snapshot payload).

- **Kind:** view (view)
- **Grain:** one version
- **Primary key:** `(paper_id, version_no)`
- **Unique constraints:** (paper_id, version_no)
- **Update strategy:** view
- **Upstream:** `dwd.paper_version`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | yes |  |
| `version_no` | integer | yes |  |
| `content_hash` | text | yes |  |
| `source_updated_at` | timestamp with time zone | yes |  |
| `valid_from` | timestamp with time zone | yes |  |
| `valid_to` | timestamp with time zone | yes |  |
| `created_at` | timestamp with time zone | yes |  |
| `warning_count` | integer | yes |  |

### `stg.stg_quarantine`

_no description_

- **Kind:** view (view)
- **Grain:** one quarantine row
- **Primary key:** `id`
- **Unique constraints:** id
- **Update strategy:** view
- **Upstream:** `meta.quarantine_record`
- **Rows:** 0

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | yes |  |
| `source` | text | yes |  |
| `source_id` | text | yes |  |
| `rule_id` | text | yes |  |
| `rule_version` | integer | yes |  |
| `status` | text | yes |  |
| `created_at` | timestamp with time zone | yes |  |
| `replayed_at` | timestamp with time zone | yes |  |

## dbt intermediate

### `int.int_issn_journal`

ISSN -> deterministic journal key (min co-listed ISSN). One-hop, identifier-only.

- **Kind:** table (table)
- **Grain:** one ISSN
- **Primary key:** `issn`
- **Unique constraints:** issn
- **Update strategy:** table: full rebuild each dbt run
- **Upstream:** `stg.stg_paper_issn`
- **Rows:** 22,033

| column | type | null | description |
|---|---|---|---|
| `issn` | text | yes |  |
| `journal_key` | text | yes |  |

### `int.int_paper_keys`

paper -> journal_key / publisher_key (NULL if no explicit identifier).

- **Kind:** table (table)
- **Grain:** one paper
- **Primary key:** `paper_id`
- **Unique constraints:** paper_id
- **Update strategy:** table: full rebuild each dbt run
- **Upstream:** `int.int_issn_journal`, `stg.stg_paper`, `stg.stg_paper_issn`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | yes |  |
| `journal_key` | text | yes |  |
| `publisher_key` | text | yes |  |

## DIM — shared dimensions

### `dim.date`

_no description_

- **Kind:** table (table)
- **Grain:** one calendar day
- **Primary key:** `date_key`
- **Unique constraints:** date_key; date_day
- **Update strategy:** table: full rebuild
- **Upstream:** —
- **Rows:** 49,673

| column | type | null | description |
|---|---|---|---|
| `date_key` | integer | yes |  |
| `date_day` | date | yes |  |
| `year` | integer | yes |  |
| `quarter` | integer | yes |  |
| `month` | integer | yes |  |
| `day` | integer | yes |  |
| `iso_weekday` | integer | yes |  |
| `is_weekend` | boolean | yes |  |

### `dim.journal`

Journal dimension keyed by ISSN-derived key. Names never merge journals.

- **Kind:** table (table)
- **Grain:** one journal key (ISSN-derived)
- **Primary key:** `journal_key`
- **Unique constraints:** journal_key
- **Update strategy:** table: full rebuild
- **Upstream:** `int.int_issn_journal`, `stg.stg_paper`, `stg.stg_paper_issn`
- **Rows:** 14,615

| column | type | null | description |
|---|---|---|---|
| `journal_key` | text | yes |  |
| `issns` | ARRAY | yes |  |
| `journal_title` | text | yes |  |
| `title_variants` | bigint | yes |  |

### `dim.publisher`

Publisher dimension keyed by Crossref member id.

- **Kind:** table (table)
- **Grain:** one Crossref member id
- **Primary key:** `publisher_key`
- **Unique constraints:** publisher_key
- **Update strategy:** table: full rebuild
- **Upstream:** `stg.stg_paper`
- **Rows:** 3,615

| column | type | null | description |
|---|---|---|---|
| `publisher_key` | text | yes |  |
| `publisher_name` | text | yes |  |
| `name_variants` | bigint | yes |  |

## DWS — summaries

### `dws.data_quality_daily`

Grain stat_date. Incremental (delete+insert on touched dates).

- **Kind:** table (incremental)
- **Grain:** one UTC day of paper creation
- **Primary key:** `stat_date`
- **Unique constraints:** stat_date
- **Update strategy:** incremental: delete+insert of days touched since watermark (lookback 1h)
- **Upstream:** `stg.stg_paper`, `stg.stg_quarantine`
- **Rows:** 1

| column | type | null | description |
|---|---|---|---|
| `stat_date` | date | yes |  |
| `papers` | bigint | yes |  |
| `with_title` | bigint | yes |  |
| `with_abstract` | bigint | yes |  |
| `with_authors` | bigint | yes |  |
| `with_references` | bigint | yes |  |
| `with_issn` | bigint | yes |  |
| `with_issued_date` | bigint | yes |  |
| `with_member` | bigint | yes |  |
| `title_rate` | numeric | yes |  |
| `abstract_rate` | numeric | yes |  |
| `authors_rate` | numeric | yes |  |
| `references_rate` | numeric | yes |  |
| `issn_rate` | numeric | yes |  |
| `quarantined_total` | bigint | yes |  |
| `quarantined_open` | bigint | yes |  |
| `_loaded_at` | timestamp with time zone | yes |  |

### `dws.journal_statistics`

Grain journal_key. Table materialization.

- **Kind:** table (table)
- **Grain:** one journal key
- **Primary key:** `journal_key`
- **Unique constraints:** journal_key
- **Update strategy:** table: full rebuild
- **Upstream:** `int.int_paper_keys`, `stg.stg_paper`
- **Rows:** 14,610

| column | type | null | description |
|---|---|---|---|
| `journal_key` | text | yes |  |
| `paper_count` | bigint | yes |  |
| `first_year` | integer | yes |  |
| `last_year` | integer | yes |  |
| `with_abstract_count` | bigint | yes |  |
| `with_authors_count` | bigint | yes |  |
| `with_references_count` | bigint | yes |  |
| `total_cited_by` | bigint | yes |  |
| `avg_reference_count` | numeric | yes |  |
| `publisher_count` | bigint | yes |  |

### `dws.paper_change_daily`

Grain change_date. Incremental over append-only paper_version.

- **Kind:** table (incremental)
- **Grain:** one UTC day of version writes
- **Primary key:** `change_date`
- **Unique constraints:** change_date
- **Update strategy:** incremental: append-only source, re-count from last day
- **Upstream:** `stg.stg_paper_version`
- **Rows:** 1

| column | type | null | description |
|---|---|---|---|
| `change_date` | date | yes |  |
| `new_papers` | bigint | yes |  |
| `new_versions` | bigint | yes |  |
| `versions_written` | bigint | yes |  |
| `papers_touched` | bigint | yes |  |
| `warnings` | bigint | yes |  |
| `_loaded_at` | timestamp with time zone | yes |  |

### `dws.publication_year_statistics`

Grain issued_year. Table materialization.

- **Kind:** table (table)
- **Grain:** one publication year
- **Primary key:** `issued_year`
- **Unique constraints:** issued_year
- **Update strategy:** table: full rebuild
- **Upstream:** `stg.stg_paper`
- **Rows:** 147

| column | type | null | description |
|---|---|---|---|
| `issued_year` | integer | yes |  |
| `paper_count` | bigint | yes |  |
| `with_abstract_count` | bigint | yes |  |
| `with_authors_count` | bigint | yes |  |
| `with_references_count` | bigint | yes |  |
| `avg_author_count` | numeric | yes |  |
| `avg_reference_count` | numeric | yes |  |
| `total_cited_by` | bigint | yes |  |

### `dws.publisher_statistics`

Grain publisher_key. Table materialization.

- **Kind:** table (table)
- **Grain:** one publisher key
- **Primary key:** `publisher_key`
- **Unique constraints:** publisher_key
- **Update strategy:** table: full rebuild
- **Upstream:** `int.int_paper_keys`, `stg.stg_paper`
- **Rows:** 3,615

| column | type | null | description |
|---|---|---|---|
| `publisher_key` | text | yes |  |
| `paper_count` | bigint | yes |  |
| `journal_count` | bigint | yes |  |
| `first_year` | integer | yes |  |
| `last_year` | integer | yes |  |
| `with_abstract_count` | bigint | yes |  |
| `total_cited_by` | bigint | yes |  |

## ADS — application layer

### `ads.paper_overview`

Single-row headline numbers (view).

- **Kind:** view (view)
- **Grain:** single row
- **Primary key:** `-`
- **Unique constraints:** -
- **Update strategy:** view
- **Upstream:** `dim.journal`, `dim.publisher`, `dwd.paper_author`, `dwd.paper_reference`, `dwd.paper_version`, `ods.crossref_works`, `stg.stg_paper`, `stg.stg_quarantine`
- **Rows:** 1

| column | type | null | description |
|---|---|---|---|
| `raw_records` | bigint | yes |  |
| `papers` | bigint | yes |  |
| `versions` | bigint | yes |  |
| `authors` | bigint | yes |  |
| `paper_references` | bigint | yes |  |
| `open_quarantine` | bigint | yes |  |
| `journals` | bigint | yes |  |
| `publishers` | bigint | yes |  |
| `min_year` | integer | yes |  |
| `max_year` | integer | yes |  |
| `last_ingested_at` | timestamp with time zone | yes |  |

### `ads.paper_search`

Grain paper_id. Search table with weighted tsvector + GIN index.

- **Kind:** table (table)
- **Grain:** one paper
- **Primary key:** `paper_id`
- **Unique constraints:** paper_id
- **Update strategy:** table: full rebuild; GIN index on search_tsv
- **Upstream:** `dwd.paper_author`, `int.int_paper_keys`, `stg.stg_paper`
- **Rows:** 101,000

| column | type | null | description |
|---|---|---|---|
| `paper_id` | uuid | yes |  |
| `doi` | text | yes |  |
| `title` | text | yes |  |
| `journal_title` | text | yes |  |
| `publisher` | text | yes |  |
| `journal_key` | text | yes |  |
| `publisher_key` | text | yes |  |
| `issued_year` | integer | yes |  |
| `type` | text | yes |  |
| `cited_by_count` | integer | yes |  |
| `author_count` | integer | yes |  |
| `has_abstract` | boolean | yes |  |
| `author_names` | text | yes |  |
| `search_tsv` | tsvector | yes |  |

### `ads.quality_overview`

Latest quality run per rule (view over meta.quality_result).

- **Kind:** view (view)
- **Grain:** one rule (latest result)
- **Primary key:** `rule_id`
- **Unique constraints:** rule_id
- **Update strategy:** view over meta.quality_result
- **Upstream:** `meta.quality_result`, `meta.quality_rule`
- **Rows:** 91

| column | type | null | description |
|---|---|---|---|
| `rule_id` | text | yes |  |
| `rule_version` | integer | yes |  |
| `dimension` | text | yes |  |
| `severity` | text | yes |  |
| `dataset` | text | yes |  |
| `description` | text | yes |  |
| `engine` | text | yes |  |
| `total_rows` | bigint | yes |  |
| `failed_rows` | bigint | yes |  |
| `passed` | boolean | yes |  |
| `pass_rate` | numeric | yes |  |
| `run_at` | timestamp with time zone | yes |  |
| `run_id` | uuid | yes |  |

## META — governance

### `meta.dataset`

Data catalog

- **Kind:** table
- **Grain:** one dataset
- **Primary key:** `dataset_id`
- **Unique constraints:** dataset_id
- **Update strategy:** refreshed by `catalog refresh`
- **Upstream:** —
- **Rows:** 40

| column | type | null | description |
|---|---|---|---|
| `dataset_id` | text | no |  |
| `layer` | text | no |  |
| `kind` | text | no |  |
| `description` | text | yes |  |
| `grain` | text | yes |  |
| `primary_key` | text | yes |  |
| `unique_constraints` | text | yes |  |
| `update_strategy` | text | yes |  |
| `materialization` | text | yes |  |
| `owner` | text | no |  |
| `row_count` | bigint | yes |  |
| `last_data_at` | timestamp with time zone | yes |  |
| `columns` | jsonb | no |  |
| `refreshed_at` | timestamp with time zone | yes |  |
| `created_at` | timestamp with time zone | no |  |

### `meta.lineage_edge`

Dataset-level lineage edges (dbt manifest + runtime registration)

- **Kind:** table
- **Grain:** one edge per origin
- **Primary key:** `id`
- **Unique constraints:** (upstream, downstream, source)
- **Update strategy:** upsert / replace per origin
- **Upstream:** —
- **Rows:** 59

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | no |  |
| `upstream` | text | no |  |
| `downstream` | text | no |  |
| `source` | text | no |  |
| `task_id` | text | no |  |
| `last_task_run_id` | uuid | yes |  |
| `first_seen_at` | timestamp with time zone | no |  |
| `last_seen_at` | timestamp with time zone | no |  |

### `meta.pipeline_run`

Pipeline executions

- **Kind:** table
- **Grain:** one run
- **Primary key:** `run_id`
- **Unique constraints:** (dag_id, external_run_id)
- **Update strategy:** status machine
- **Upstream:** —
- **Rows:** 31

| column | type | null | description |
|---|---|---|---|
| `run_id` | uuid | no |  |
| `dag_id` | text | no |  |
| `external_run_id` | text | yes |  |
| `trigger` | text | no |  |
| `status` | text | no |  |
| `params` | jsonb | no |  |
| `started_at` | timestamp with time zone | no |  |
| `ended_at` | timestamp with time zone | yes |  |

### `meta.quality_result`

Quality rule execution records

- **Kind:** table
- **Grain:** one rule execution
- **Primary key:** `id`
- **Unique constraints:** -
- **Update strategy:** append-only
- **Upstream:** `dwd.paper`, `meta.quarantine_record`, `ods.crossref_works`
- **Rows:** 693

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | no |  |
| `run_id` | uuid | no |  |
| `rule_id` | text | no |  |
| `rule_version` | integer | no |  |
| `dataset` | text | no |  |
| `severity` | text | no |  |
| `total_rows` | bigint | no |  |
| `failed_rows` | bigint | no |  |
| `passed` | boolean | yes |  |
| `details` | jsonb | no |  |
| `run_at` | timestamp with time zone | no |  |

### `meta.quality_rule`

Quality rule catalog

- **Kind:** table
- **Grain:** one rule version
- **Primary key:** `(rule_id, version)`
- **Unique constraints:** (rule_id, version)
- **Update strategy:** upsert on sync
- **Upstream:** —
- **Rows:** 91

| column | type | null | description |
|---|---|---|---|
| `rule_id` | text | no |  |
| `version` | integer | no |  |
| `dimension` | text | no |  |
| `severity` | text | no |  |
| `dataset` | text | no |  |
| `description` | text | no |  |
| `engine` | text | no |  |

### `meta.quarantine_record`

Records rejected by Error rules, with raw payload, rule, version, status

- **Kind:** table
- **Grain:** one rejected record per rule
- **Primary key:** `id`
- **Unique constraints:** open (source, source_id, rule_id, payload_hash)
- **Update strategy:** status open -> replayed|discarded
- **Upstream:** `kafka.paper.raw.events`, `raw.crossref_jsonl`
- **Rows:** 0

| column | type | null | description |
|---|---|---|---|
| `id` | bigint | no |  |
| `source` | text | no |  |
| `source_id` | text | yes |  |
| `raw_file_id` | bigint | yes |  |
| `raw_line` | integer | yes |  |
| `payload` | jsonb | no |  |
| `rule_id` | text | no |  |
| `rule_version` | integer | no |  |
| `reason` | text | no |  |
| `status` | text | no |  |
| `created_at` | timestamp with time zone | no |  |
| `replayed_at` | timestamp with time zone | yes |  |
| `last_replay_error` | text | yes |  |
| `payload_hash` | text | yes |  |

### `meta.sync_checkpoint`

Ingestion window checkpoints and per-source watermark

- **Kind:** table
- **Grain:** one (batch, unit) or watermark
- **Primary key:** `(source, batch_id, unit_key)`
- **Unique constraints:** (source, batch_id, unit_key)
- **Update strategy:** updated per page
- **Upstream:** —
- **Rows:** 93

| column | type | null | description |
|---|---|---|---|
| `source` | text | no |  |
| `batch_id` | text | no |  |
| `unit_key` | text | no |  |
| `status` | text | no |  |
| `window_from` | date | yes |  |
| `window_until` | date | yes |  |
| `cursor` | text | yes |  |
| `parts_done` | integer | no |  |
| `rows_seen` | bigint | no |  |
| `rows_inserted` | bigint | no |  |
| `rows_updated` | bigint | no |  |
| `rows_unchanged` | bigint | no |  |
| `rows_skipped` | bigint | no |  |
| `attempt` | integer | no |  |
| `error` | text | yes |  |
| `updated_at` | timestamp with time zone | no |  |

### `meta.task_run`

Task attempts with row counts, data version, duration

- **Kind:** table
- **Grain:** one task attempt
- **Primary key:** `task_run_id`
- **Unique constraints:** -
- **Update strategy:** status machine
- **Upstream:** —
- **Rows:** 57

| column | type | null | description |
|---|---|---|---|
| `task_run_id` | uuid | no |  |
| `run_id` | uuid | no |  |
| `task_id` | text | no |  |
| `attempt` | integer | no |  |
| `status` | text | no |  |
| `rows_in` | bigint | yes |  |
| `rows_out` | bigint | yes |  |
| `rows_failed` | bigint | yes |  |
| `data_version` | text | yes |  |
| `started_at` | timestamp with time zone | no |  |
| `ended_at` | timestamp with time zone | yes |  |
| `duration_ms` | bigint | yes |  |
| `error` | text | yes |  |
| `details` | jsonb | no |  |
