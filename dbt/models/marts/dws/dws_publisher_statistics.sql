{{ config(materialized='table') }}
-- Grain: publisher_key (Crossref member id).
select
    k.publisher_key,
    count(*)                                          as paper_count,
    count(distinct k.journal_key)                     as journal_count,
    min(p.issued_year)                                as first_year,
    max(p.issued_year)                                as last_year,
    count(*) filter (where p.has_abstract)            as with_abstract_count,
    coalesce(sum(p.cited_by_count), 0)                as total_cited_by
from {{ ref('int_paper_keys') }} k
join {{ ref('stg_paper') }} p using (paper_id)
where k.publisher_key is not null
group by k.publisher_key
