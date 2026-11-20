-- Each paper has exactly one open-ended version, and its current version row matches dwd.paper.
select paper_id from {{ source('dwd', 'paper_version') }} where valid_to is null group by 1 having count(*) <> 1
union all
select paper_id from {{ source('dwd', 'paper_version') }} where valid_to is not null and valid_to < valid_from
union all
select p.paper_id from {{ source('dwd', 'paper') }} p
where not exists (select 1 from {{ source('dwd', 'paper_version') }} v
                  where v.paper_id = p.paper_id and v.version_no = p.current_version and v.content_hash = p.content_hash)
