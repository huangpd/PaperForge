-- arXiv metadata snapshot as a second source.
-- Identity: dwd.paper gets a source-neutral key. paper_key = DOI when the paper has one, else 'arxiv:<id>'.
-- primary_doi stays a real DOI (nullable, unique when present).

-- ODS: latest source-native record per arXiv id (same shape as ods.crossref_works, keyed by arxiv_id).
CREATE TABLE ods.arxiv_works (
  arxiv_id          text PRIMARY KEY,
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
  published_at      timestamptz
);
CREATE INDEX ON ods.arxiv_works (batch_id);

ALTER TABLE dwd.paper ADD COLUMN paper_key text;
UPDATE dwd.paper SET paper_key = primary_doi;
ALTER TABLE dwd.paper ALTER COLUMN paper_key SET NOT NULL;
ALTER TABLE dwd.paper ALTER COLUMN primary_doi DROP NOT NULL;
ALTER TABLE dwd.paper ADD CONSTRAINT uq_paper_key UNIQUE (paper_key);

-- Which source produced a version. A paper that has any Crossref version is "owned" by Crossref:
-- later arXiv records for it only enrich dwd.paper_arxiv and never write versions.
ALTER TABLE dwd.paper_version ADD COLUMN source text NOT NULL DEFAULT 'crossref';

-- Grain: one arXiv submission (arxiv_id). Several arXiv ids may point to one paper (same DOI).
CREATE TABLE dwd.paper_arxiv (
  arxiv_id         text PRIMARY KEY,
  paper_id         uuid NOT NULL REFERENCES dwd.paper ON DELETE CASCADE,
  doi              text,
  primary_category text NOT NULL,
  categories       text[] NOT NULL,
  title            text,
  abstract         text,
  journal_ref      text,
  comments         text,
  report_no        text,
  license          text,
  submitted_at     timestamptz,
  updated_date     date,
  version_count    int NOT NULL DEFAULT 1,
  content_hash     text NOT NULL,
  raw_file_id      bigint,
  raw_line         int,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON dwd.paper_arxiv (paper_id);
CREATE INDEX ON dwd.paper_arxiv USING gin (categories);
CREATE INDEX ON dwd.paper_arxiv (primary_category);
