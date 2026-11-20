{{ config(materialized='table', indexes=[{'columns': ['paper_id'], 'unique': True}, {'columns': ['search_tsv'], 'type': 'gin'},
                                          {'columns': ['doi']}, {'columns': ['issued_year']}, {'columns': ['arxiv_id']},
                                          {'columns': ['categories'], 'type': 'gin'}]) }}
-- Grain: paper_id. Denormalised for API search; rebuilt on every run.
with authors as (
    select paper_id, string_agg(coalesce(family, name), ' ' order by ordinal) as author_names
    from {{ source('dwd', 'paper_author') }}
    group by paper_id
)
-- One arXiv row per paper (the earliest id) when the paper has several.
, arx as (
    select distinct on (paper_id) paper_id, arxiv_id, primary_category, categories, abstract is not null as arxiv_has_abstract
    from {{ source('dwd', 'paper_arxiv') }}
    order by paper_id, arxiv_id
)
select
    p.paper_id, p.paper_key, p.doi, x.arxiv_id, x.primary_category, x.categories, p.title, p.container_title as journal_title, p.publisher, k.journal_key, k.publisher_key,
    p.issued_year, p.type, p.cited_by_count, p.author_count, p.has_abstract or coalesce(x.arxiv_has_abstract, false) as has_abstract, coalesce(a.author_names, '') as author_names,
    setweight(to_tsvector('simple', coalesce(p.title, '')), 'A')
      || setweight(to_tsvector('simple', coalesce(p.container_title, '')), 'B')
      || setweight(to_tsvector('simple', coalesce(a.author_names, '')), 'C') as search_tsv
from {{ ref('stg_paper') }} p
join {{ ref('int_paper_keys') }} k using (paper_id)
left join authors a using (paper_id)
left join arx x using (paper_id)
