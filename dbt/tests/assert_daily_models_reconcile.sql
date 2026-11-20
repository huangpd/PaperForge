-- Incremental daily models must agree with their detail tables.
select 'paper_count' as what from (select (select coalesce(sum(papers), 0) from {{ ref('dws_data_quality_daily') }}) as d,
                                           (select count(*) from {{ ref('stg_paper') }}) as p) x where d <> p
union all
select 'version_count' from (select (select coalesce(sum(versions_written), 0) from {{ ref('dws_paper_change_daily') }}) as d,
                                    (select count(*) from {{ ref('stg_paper_version') }}) as p) y where d <> p
