CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS meta;

-- Grain: one row per ingestion run. PK batch_id. Status machine: running -> completed | failed.
CREATE TABLE ods.ingestion_batch (
  batch_id     text PRIMARY KEY,
  source       text NOT NULL,
  mode         text NOT NULL CHECK (mode IN ('bootstrap','incremental')),
  status       text NOT NULL CHECK (status IN ('running','completed','failed')),
  params       jsonb NOT NULL,
  rows_seen bigint NOT NULL DEFAULT 0, rows_inserted bigint NOT NULL DEFAULT 0,
  rows_updated bigint NOT NULL DEFAULT 0, rows_unchanged bigint NOT NULL DEFAULT 0,
  rows_skipped bigint NOT NULL DEFAULT 0,
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  error        text
);

-- Grain: one row per immutable raw file. Append-only (rows never updated).
CREATE TABLE ods.raw_file_manifest (
  file_id      bigserial PRIMARY KEY,
  batch_id     text NOT NULL REFERENCES ods.ingestion_batch,
  unit_key     text NOT NULL,
  seq          int  NOT NULL,
  path         text NOT NULL UNIQUE,
  record_count int  NOT NULL,
  size_bytes   bigint NOT NULL,
  sha256       text NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (batch_id, unit_key, seq)
);

-- Grain: one row per DOI = latest source-native record. History lives in raw files.
CREATE TABLE ods.crossref_works (
  doi_norm          text PRIMARY KEY,
  source            text NOT NULL,
  source_id         text NOT NULL,
  payload           jsonb NOT NULL,
  payload_hash      text NOT NULL,
  source_updated_at timestamptz,
  batch_id          text NOT NULL REFERENCES ods.ingestion_batch,
  raw_file_id       bigint NOT NULL REFERENCES ods.raw_file_manifest,
  raw_line          int NOT NULL,
  first_seen_at     timestamptz NOT NULL DEFAULT now(),
  ingested_at       timestamptz NOT NULL DEFAULT now(),
  published_at      timestamptz            -- Kafka outbox marker (stage 4)
);
CREATE INDEX ON ods.crossref_works (batch_id);

-- Grain: one row per HTTP request (API connector).
CREATE TABLE ods.ingestion_request (
  id          bigserial PRIMARY KEY,
  batch_id    text NOT NULL,
  unit_key    text NOT NULL,
  status_code int,
  elapsed_ms  double precision NOT NULL,
  attempt     int NOT NULL,
  error       text,
  at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON ods.ingestion_request (batch_id);

-- Grain: one row per (batch, work unit/time window); plus one 'watermark' row per source.
CREATE TABLE meta.sync_checkpoint (
  source       text NOT NULL,
  batch_id     text NOT NULL DEFAULT '',   -- '' for the watermark row
  unit_key     text NOT NULL,
  status       text NOT NULL CHECK (status IN ('pending','running','done','failed')),
  window_from  date, window_until date,
  cursor       text,
  parts_done   int NOT NULL DEFAULT 0,
  rows_seen bigint NOT NULL DEFAULT 0, rows_inserted bigint NOT NULL DEFAULT 0,
  rows_updated bigint NOT NULL DEFAULT 0, rows_unchanged bigint NOT NULL DEFAULT 0,
  rows_skipped bigint NOT NULL DEFAULT 0,
  attempt      int NOT NULL DEFAULT 0,
  error        text,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, batch_id, unit_key)
);
