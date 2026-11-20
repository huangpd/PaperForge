-- Streaming is at-least-once: a redelivered invalid event must not create another open quarantine row.
ALTER TABLE meta.quarantine_record ADD COLUMN IF NOT EXISTS payload_hash text;
CREATE UNIQUE INDEX IF NOT EXISTS uq_quarantine_open_record
  ON meta.quarantine_record (source, coalesce(source_id, ''), rule_id, payload_hash)
  WHERE status = 'open' AND payload_hash IS NOT NULL;
