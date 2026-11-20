{{ config(materialized='table') }}
-- Grain: journal_key. Updated by full rebuild (table).
select
    k.journal_key,
    count(*)                                          as paper_count,
    min(p.issued_year)                                as first_year,
    max(p.issued_year)                                as last_year,
    count(*) filter (where p.has_abstract)            as with_abstract_count,
    count(*) filter (where p.has_authors)             as with_authors_count,
    count(*) filter (where p.has_references)          as with_references_count,
    coalesce(sum(p.cited_by_count), 0)                as total_cited_by,
    round(avg(p.reference_count), 2)                  as avg_reference_count,
    count(distinct k.publisher_key)                   as publisher_count
from {{ ref('int_paper_keys') }} k
join {{ ref('stg_paper') }} p using (paper_id)
where k.journal_key is not null
group by k.journal_key
