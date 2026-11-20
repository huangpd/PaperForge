{{ config(materialized='table') }}
-- Grain: issued_year (papers without a valid publication date are excluded; they are counted in data quality).
select
    issued_year,
    count(*)                                          as paper_count,
    count(*) filter (where has_abstract)              as with_abstract_count,
    count(*) filter (where has_authors)               as with_authors_count,
    count(*) filter (where has_references)            as with_references_count,
    round(avg(author_count), 2)                       as avg_author_count,
    round(avg(reference_count), 2)                    as avg_reference_count,
    coalesce(sum(cited_by_count), 0)                  as total_cited_by
from {{ ref('stg_paper') }}
where issued_year is not null
group by issued_year
