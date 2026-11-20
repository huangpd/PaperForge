-- Aggregates must add up to the detail: journal stats + papers without a journal = all papers; same for years.
with t as (
  select (select count(*) from {{ ref('stg_paper') }}) as papers,
         (select coalesce(sum(paper_count), 0) from {{ ref('dws_journal_statistics') }}) as in_journals,
         (select count(*) from {{ ref('int_paper_keys') }} where journal_key is null) as no_journal,
         (select coalesce(sum(paper_count), 0) from {{ ref('dws_publication_year_statistics') }}) as in_years,
         (select count(*) from {{ ref('stg_paper') }} where issued_year is null) as no_year,
         (select coalesce(sum(paper_count), 0) from {{ ref('dws_publisher_statistics') }}) as in_publishers,
         (select count(*) from {{ ref('int_paper_keys') }} where publisher_key is null) as no_publisher
)
select * from t where papers <> in_journals + no_journal or papers <> in_years + no_year or papers <> in_publishers + no_publisher
