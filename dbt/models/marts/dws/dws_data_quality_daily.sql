{{ config(materialized='incremental', unique_key='stat_date', incremental_strategy='delete+insert') }}
-- Grain: stat_date = UTC date a paper entity was created (or a record quarantined).
-- Incremental: recompute every date touched by rows changed since the watermark (idempotent delete+insert).
with touched as (
    select distinct created_at::date as stat_date from {{ ref('stg_paper') }}
    {% if is_incremental() %} where updated_at >= {{ incremental_watermark() }} {% endif %}
    union
    select distinct created_at::date from {{ ref('stg_quarantine') }}
    {% if is_incremental() %} where greatest(created_at, coalesce(replayed_at, created_at)) >= {{ incremental_watermark() }} {% endif %}
),
paper_daily as (
    select p.created_at::date as stat_date,
           count(*)                                  as papers,
           count(*) filter (where has_title)         as with_title,
           count(*) filter (where has_abstract)      as with_abstract,
           count(*) filter (where has_authors)       as with_authors,
           count(*) filter (where has_references)    as with_references,
           count(*) filter (where has_issn)          as with_issn,
           count(*) filter (where has_issued_date)   as with_issued_date,
           count(*) filter (where has_member)        as with_member
    from {{ ref('stg_paper') }} p
    where p.created_at::date in (select stat_date from touched)
    group by 1
),
quarantine_daily as (
    select created_at::date as stat_date,
           count(*)                                  as quarantined_total,
           count(*) filter (where status = 'open')   as quarantined_open
    from {{ ref('stg_quarantine') }}
    where created_at::date in (select stat_date from touched)
    group by 1
)
select
    t.stat_date,
    coalesce(p.papers, 0)            as papers,
    coalesce(p.with_title, 0)        as with_title,
    coalesce(p.with_abstract, 0)     as with_abstract,
    coalesce(p.with_authors, 0)      as with_authors,
    coalesce(p.with_references, 0)   as with_references,
    coalesce(p.with_issn, 0)         as with_issn,
    coalesce(p.with_issued_date, 0)  as with_issued_date,
    coalesce(p.with_member, 0)       as with_member,
    {{ rate('p.with_title', 'p.papers') }}        as title_rate,
    {{ rate('p.with_abstract', 'p.papers') }}     as abstract_rate,
    {{ rate('p.with_authors', 'p.papers') }}      as authors_rate,
    {{ rate('p.with_references', 'p.papers') }}   as references_rate,
    {{ rate('p.with_issn', 'p.papers') }}         as issn_rate,
    coalesce(q.quarantined_total, 0) as quarantined_total,
    coalesce(q.quarantined_open, 0)  as quarantined_open,
    now()                            as _loaded_at
from touched t
left join paper_daily p using (stat_date)
left join quarantine_daily q using (stat_date)
