# Entity-relationship diagrams

Generated from the foreign keys and primary keys in PostgreSQL (`scripts/gen_docs.py`). Only key columns are shown; see the data dictionary for all columns.

## Raw layer and ingestion bookkeeping (ods, meta.sync_checkpoint)

```mermaid
erDiagram
  crossref_works {
    text doi_norm PK
    text payload_hash
    text batch_id FK
    bigint raw_file_id FK
  }
  ingestion_batch {
    text batch_id PK
    text status
  }
  ingestion_request {
    bigint id PK
  }
  raw_file_manifest {
    bigint file_id PK
    text batch_id FK
  }
  sync_checkpoint {
    text source PK
    text batch_id PK
    text unit_key PK
    text status
  }
  ingestion_batch ||--o{ crossref_works : "crossref_works→ingestion_batch"
  raw_file_manifest ||--o{ crossref_works : "crossref_works→raw_file_manifest"
  ingestion_batch ||--o{ raw_file_manifest : "raw_file_manifest→ingestion_batch"
```

## Standardized layer (dwd)

```mermaid
erDiagram
  paper {
    uuid paper_id PK
    text primary_doi
    text content_hash
  }
  paper_author {
    uuid paper_id PK
    integer ordinal PK
  }
  paper_identifier {
    bigint id PK
    uuid paper_id FK
    text status
  }
  paper_identity_event {
    uuid event_id PK
  }
  paper_reference {
    uuid paper_id PK
    integer ordinal PK
  }
  paper_version {
    uuid paper_id PK
    integer version_no PK
    text content_hash
  }
  paper ||--o{ paper_author : "paper_author→paper"
  paper ||--o{ paper_identifier : "paper_identifier→paper"
  paper ||--o{ paper_reference : "paper_reference→paper"
  paper ||--o{ paper_version : "paper_version→paper"
```

## Governance (meta)

```mermaid
erDiagram
  dataset {
    text dataset_id PK
  }
  lineage_edge {
    bigint id PK
    text upstream FK
    text downstream FK
    uuid last_task_run_id FK
  }
  pipeline_run {
    uuid run_id PK
    text status
  }
  quality_result {
    bigint id PK
    text rule_id FK
    integer rule_version FK
  }
  quality_rule {
    text rule_id PK
    integer version PK
  }
  quarantine_record {
    bigint id PK
    text status
    text payload_hash
  }
  schema_migrations {
    text name PK
  }
  sync_checkpoint {
    text source PK
    text batch_id PK
    text unit_key PK
    text status
  }
  task_run {
    uuid task_run_id PK
    uuid run_id FK
    text status
  }
  dataset ||--o{ lineage_edge : "lineage_edge→dataset"
  dataset ||--o{ lineage_edge : "lineage_edge→dataset"
  task_run ||--o{ lineage_edge : "lineage_edge→task_run"
  quality_rule ||--o{ quality_result : "quality_result→quality_rule"
  pipeline_run ||--o{ task_run : "task_run→pipeline_run"
```

## Logical flow between layers

```mermaid
flowchart LR
  CR[(Crossref API / offline dump)] --> RAW[raw JSONL + parquet]
  RAW --> ODS[ods.crossref_works]
  ODS --> K{{Kafka paper.raw.events}}
  K --> DWD[dwd.paper / version / author / reference / identifier]
  RAW -. batch replay .-> DWD
  DWD --> DIM[dim.journal / publisher / date]
  DWD --> DWS[dws.* summaries]
  DIM --> DWS
  DWS --> ADS[ads.paper_search / overview / quality]
  DWD --> ADS
```
