-- one row per (paper, ISSN)
select p.paper_id, i.issn
from {{ source('dwd', 'paper') }} p
cross join lateral unnest(p.issns) as i(issn)
