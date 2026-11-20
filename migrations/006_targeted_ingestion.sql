-- Targeted ingestion: fetch specific records by identifier (e.g. one DOI) instead of a window or a sample.
ALTER TABLE ods.ingestion_batch DROP CONSTRAINT IF EXISTS ingestion_batch_mode_check;
ALTER TABLE ods.ingestion_batch ADD CONSTRAINT ingestion_batch_mode_check CHECK (mode IN ('bootstrap','incremental','targeted'));
