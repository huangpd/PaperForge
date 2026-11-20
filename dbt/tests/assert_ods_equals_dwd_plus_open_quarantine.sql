-- Conservation, per source. Returns one row per violation.
--   crossref: every ODS DOI is a paper (by primary DOI) or has an open quarantine record
--   arxiv:    every ODS arXiv id has a dwd.paper_arxiv row or an open quarantine record, and no dwd.paper_arxiv row lacks its ODS record
select 'crossref' as source, w.doi_norm as key from {{ source('ods', 'crossref_works') }} w
where not exists (select 1 from {{ source('dwd', 'paper') }} p where p.primary_doi = w.doi_norm)
  and not exists (select 1 from {{ source('meta', 'quarantine_record') }} q where q.status = 'open' and q.source_id = w.doi_norm)
union all
select 'arxiv', w.arxiv_id from {{ source('ods', 'arxiv_works') }} w
where not exists (select 1 from {{ source('dwd', 'paper_arxiv') }} a where a.arxiv_id = w.arxiv_id)
  and not exists (select 1 from {{ source('meta', 'quarantine_record') }} q where q.status = 'open' and q.source_id = w.arxiv_id)
union all
select 'arxiv_without_ods', a.arxiv_id from {{ source('dwd', 'paper_arxiv') }} a
where not exists (select 1 from {{ source('ods', 'arxiv_works') }} w where w.arxiv_id = a.arxiv_id)
