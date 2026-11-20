CREATE SCHEMA IF NOT EXISTS dwd;

-- Grain: one row per paper entity (current state). paper_id is the permanent internal key (UUIDv7).
CREATE TABLE dwd.paper (
  paper_id          uuid PRIMARY KEY,
  primary_doi       text NOT NULL,
  title text, abstract text, container_title text, publisher text, member text,
  type text, language text, volume text, issue text, page text,
  issns             text[] NOT NULL DEFAULT '{}',
  issued_date       date, issued_year int, issued_precision text,   -- year|month|day
  reference_count   int, cited_by_count int, author_count int NOT NULL DEFAULT 0,
  current_version   int NOT NULL,
  content_hash      text NOT NULL,
  source_updated_at timestamptz NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_paper_primary_doi UNIQUE (primary_doi)
);
CREATE INDEX ON dwd.paper (issued_year);
CREATE INDEX ON dwd.paper (publisher);

-- Grain: one external identifier of a paper, with validity interval.
CREATE TABLE dwd.paper_identifier (
  id          bigserial PRIMARY KEY,
  paper_id    uuid NOT NULL REFERENCES dwd.paper ON DELETE CASCADE,
  scheme      text NOT NULL,                       -- doi (MVP); openalex_work_id, pmid ... later
  value       text NOT NULL,
  source      text NOT NULL,
  valid_from  timestamptz NOT NULL,
  valid_to    timestamptz,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','retired','conflict'))
);
CREATE UNIQUE INDEX uq_identifier_active ON dwd.paper_identifier (scheme, value, source) WHERE valid_to IS NULL;
CREATE INDEX ON dwd.paper_identifier (paper_id);

-- Grain: one version = one distinct content on the source_updated_at timeline.
-- version_no is assigned in arrival order; logical order is (source_updated_at, version_no).
CREATE TABLE dwd.paper_version (
  paper_id           uuid NOT NULL REFERENCES dwd.paper ON DELETE CASCADE,
  version_no         int  NOT NULL,
  content_hash       text NOT NULL,
  snapshot           jsonb NOT NULL,               -- normalized content incl. authors & references
  source_updated_at  timestamptz NOT NULL,
  valid_from         timestamptz NOT NULL,         -- = source_updated_at
  valid_to           timestamptz,                  -- start of the next newer version; NULL = current
  raw_file_id        bigint, raw_line int,         -- trace back to the raw record
  normalizer_version int NOT NULL,
  quality_flags      jsonb NOT NULL DEFAULT '[]',  -- warnings kept with the version
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (paper_id, version_no),
  CONSTRAINT uq_version_point UNIQUE (paper_id, source_updated_at, content_hash)
);

-- Grain: one author of the *current* version of a paper.
CREATE TABLE dwd.paper_author (
  paper_id uuid NOT NULL REFERENCES dwd.paper ON DELETE CASCADE,
  ordinal int NOT NULL,
  given text, family text, name text, orcid text, sequence text,
  affiliations text[] NOT NULL DEFAULT '{}',
  PRIMARY KEY (paper_id, ordinal)
);

-- Grain: one reference of the *current* version of a paper.
CREATE TABLE dwd.paper_reference (
  paper_id uuid NOT NULL REFERENCES dwd.paper ON DELETE CASCADE,
  ordinal int NOT NULL,
  ref_key text, ref_doi text, unstructured text, article_title text, journal_title text, ref_year int,
  PRIMARY KEY (paper_id, ordinal)
);
CREATE INDEX ON dwd.paper_reference (ref_doi);

-- Grain: one identity event (merge/split/identifier change/conflict). Reserved; no algorithm in MVP.
CREATE TABLE dwd.paper_identity_event (
  event_id      uuid PRIMARY KEY,
  event_type    text NOT NULL CHECK (event_type IN ('identifier_change','merge','split','conflict')),
  from_paper_id uuid, to_paper_id uuid,
  payload       jsonb NOT NULL DEFAULT '{}',
  occurred_at   timestamptz NOT NULL,
  recorded_at   timestamptz NOT NULL DEFAULT now()
);

-- Records rejected by Error-level rules. Keeps the raw record, reason, rule version and replay status.
CREATE TABLE meta.quarantine_record (
  id            bigserial PRIMARY KEY,
  source        text NOT NULL,
  source_id     text,
  raw_file_id   bigint, raw_line int,
  payload       jsonb NOT NULL,
  rule_id       text NOT NULL,
  rule_version  int NOT NULL,
  reason        text NOT NULL,
  status        text NOT NULL DEFAULT 'open' CHECK (status IN ('open','replayed','discarded')),
  created_at    timestamptz NOT NULL DEFAULT now(),
  replayed_at   timestamptz
);
CREATE INDEX ON meta.quarantine_record (status, rule_id);
