-- Grain: one dataset (table, view, topic, file set or external system). The catalog.
CREATE TABLE meta.dataset (
  dataset_id       text PRIMARY KEY,               -- e.g. ods.crossref_works, kafka.paper.raw.events, external.crossref_api
  layer            text NOT NULL CHECK (layer IN ('source','raw','kafka','ods','dwd','dim','dws','ads','meta','stg','int')),
  kind             text NOT NULL CHECK (kind IN ('table','view','topic','files','external')),
  description      text,
  grain            text,
  primary_key      text,
  unique_constraints text,
  update_strategy  text,
  materialization  text,
  owner            text NOT NULL DEFAULT 'paperforge',
  row_count        bigint,
  last_data_at     timestamptz,                    -- newest row timestamp known for the dataset
  columns          jsonb NOT NULL DEFAULT '[]',    -- [{name,type,nullable,description}]
  refreshed_at     timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now()
);

-- Grain: one orchestrated or CLI-invoked pipeline execution.
CREATE TABLE meta.pipeline_run (
  run_id          uuid PRIMARY KEY,
  dag_id          text NOT NULL,                   -- Airflow dag id or 'cli'
  external_run_id text,                            -- Airflow run_id; NULL for CLI runs
  trigger         text NOT NULL DEFAULT 'manual',
  status          text NOT NULL CHECK (status IN ('running','success','failed')),
  params          jsonb NOT NULL DEFAULT '{}',
  started_at      timestamptz NOT NULL DEFAULT now(),
  ended_at        timestamptz,
  UNIQUE (dag_id, external_run_id)
);
CREATE INDEX ON meta.pipeline_run (started_at DESC);

-- Grain: one task attempt inside a pipeline run.
CREATE TABLE meta.task_run (
  task_run_id   uuid PRIMARY KEY,
  run_id        uuid NOT NULL REFERENCES meta.pipeline_run,
  task_id       text NOT NULL,
  attempt       int  NOT NULL DEFAULT 1,
  status        text NOT NULL CHECK (status IN ('running','success','failed')),
  rows_in       bigint, rows_out bigint, rows_failed bigint,
  data_version  text,                              -- e.g. ingestion batch id, quality run id, dbt invocation id
  started_at    timestamptz NOT NULL DEFAULT now(),
  ended_at      timestamptz,
  duration_ms   bigint,
  error         text,
  details       jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX ON meta.task_run (run_id);
CREATE INDEX ON meta.task_run (task_id, started_at DESC);

-- Grain: one directed dataset-level dependency (upstream -> downstream) per origin.
CREATE TABLE meta.lineage_edge (
  id            bigserial PRIMARY KEY,
  upstream      text NOT NULL REFERENCES meta.dataset,
  downstream    text NOT NULL REFERENCES meta.dataset,
  source        text NOT NULL CHECK (source IN ('dbt','runtime')),
  task_id       text NOT NULL,                     -- dbt model unique name, or the runtime task that moves the data
  last_task_run_id uuid REFERENCES meta.task_run,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (upstream, downstream, source),
  CHECK (upstream <> downstream)
);
CREATE INDEX ON meta.lineage_edge (downstream);
CREATE INDEX ON meta.lineage_edge (upstream);
