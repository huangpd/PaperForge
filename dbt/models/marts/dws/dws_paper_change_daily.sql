{{ config(materialized='incremental', unique_key='change_date', incremental_strategy='delete+insert') }}
-- Grain: change_date = UTC date a version row was written. paper_version is append-only, so the day
-- containing the previous watermark is re-counted and later days are added.
select
    created_at::date                                  as change_date,
    count(*) filter (where version_no = 1)            as new_papers,
    count(*) filter (where version_no > 1)            as new_versions,
    count(*)                                          as versions_written,
    count(distinct paper_id)                          as papers_touched,
    sum(warning_count)                                as warnings,
    now()                                             as _loaded_at
from {{ ref('stg_paper_version') }}
{% if is_incremental() %}
where created_at::date >= (select max(change_date) from {{ this }})
{% endif %}
group by 1
