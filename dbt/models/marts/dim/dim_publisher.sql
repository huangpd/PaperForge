-- Grain: one row per Crossref member id. Publisher names are descriptive; only the member id identifies.
select
    member                                                  as publisher_key,
    mode() within group (order by publisher)                as publisher_name,
    count(distinct publisher)                               as name_variants
from {{ ref('stg_paper') }}
where member is not null
group by member
