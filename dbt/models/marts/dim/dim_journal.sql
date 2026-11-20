-- Grain: one row per deterministic journal key (ISSN-based). Name is descriptive only; equal names are NOT merged.
select
    m.journal_key,
    array_agg(distinct m.issn order by m.issn)                              as issns,
    mode() within group (order by p.container_title)                        as journal_title,
    count(distinct p.container_title)                                       as title_variants
from {{ ref('int_issn_journal') }} m
join {{ ref('stg_paper_issn') }} i on i.issn = m.issn
join {{ ref('stg_paper') }} p on p.paper_id = i.paper_id
group by m.journal_key
