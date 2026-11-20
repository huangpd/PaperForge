-- 1:1 with dwd.paper. Only renames/derives flags; no business logic.
select
    paper_id,
    paper_key,
    primary_doi as doi,
    title, abstract, container_title, publisher, member, type, language,
    issns, issued_date, issued_year, issued_precision,
    reference_count, cited_by_count, author_count, current_version,
    source_updated_at, created_at, updated_at,
    title is not null                         as has_title,
    abstract is not null                      as has_abstract,
    author_count > 0                          as has_authors,
    coalesce(reference_count, 0) > 0          as has_references,
    cardinality(issns) > 0                    as has_issn,
    issued_date is not null                   as has_issued_date,
    member is not null                        as has_member,
    container_title is not null               as has_container_title
from {{ source('dwd', 'paper') }}
