-- Grain: one row per (rule_id, version). Catalog of quality rules.
CREATE TABLE meta.quality_rule (
  rule_id      text NOT NULL,
  version      int  NOT NULL,
  dimension    text NOT NULL CHECK (dimension IN ('completeness','uniqueness','validity','consistency','timeliness','referential_integrity','source_completeness')),
  severity     text NOT NULL CHECK (severity IN ('error','warning','metric')),
  dataset      text NOT NULL,
  description  text NOT NULL,
  engine       text NOT NULL,                       -- pandera | sql | python-validator | dbt
  PRIMARY KEY (rule_id, version)
);

-- Grain: one row per rule execution (rule x run). Append-only.
CREATE TABLE meta.quality_result (
  id           bigserial PRIMARY KEY,
  run_id       uuid NOT NULL,
  rule_id      text NOT NULL,
  rule_version int  NOT NULL,
  dataset      text NOT NULL,
  severity     text NOT NULL,
  total_rows   bigint NOT NULL,
  failed_rows  bigint NOT NULL,                     -- for metric rules: rows lacking the attribute
  passed       boolean,                             -- NULL for metric rules (no pass/fail)
  details      jsonb NOT NULL DEFAULT '{}',
  run_at       timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (rule_id, rule_version) REFERENCES meta.quality_rule
);
CREATE INDEX ON meta.quality_result (run_id);
CREATE INDEX ON meta.quality_result (rule_id, run_at DESC);
ALTER TABLE meta.quarantine_record ADD COLUMN IF NOT EXISTS last_replay_error text;
