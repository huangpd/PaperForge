select paper_id, issued_year from {{ ref('stg_paper') }} where issued_year is not null and issued_year not between 1600 and 2100
