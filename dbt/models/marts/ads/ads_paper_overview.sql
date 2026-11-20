{{ config(materialized='view') }}
-- Grain: single row. Headline numbers for the console.
select
    (select count(*) from {{ source('ods', 'crossref_works') }})
      + (select count(*) from {{ source('ods', 'arxiv_works') }})                          as raw_records,
    (select count(*) from {{ ref('stg_paper') }})                                         as papers,
    (select count(*) from {{ source('dwd', 'paper_version') }})                           as versions,
    (select count(*) from {{ source('dwd', 'paper_author') }})                            as authors,
    (select count(*) from {{ source('dwd', 'paper_reference') }})                         as paper_references,
    (select count(*) from {{ ref('stg_quarantine') }} where status = 'open')              as open_quarantine,
    (select count(*) from {{ ref('dim_journal') }})                                       as journals,
    (select count(*) from {{ ref('dim_publisher') }})                                     as publishers,
    (select min(issued_year) from {{ ref('stg_paper') }})                                 as min_year,
    (select max(issued_year) from {{ ref('stg_paper') }})                                 as max_year,
    greatest((select max(ingested_at) from {{ source('ods', 'crossref_works') }}),
             (select max(ingested_at) from {{ source('ods', 'arxiv_works') }}))               as last_ingested_at
