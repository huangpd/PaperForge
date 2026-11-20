-- paper -> dimension keys. journal_key: from ISSNs; publisher_key: Crossref member id. NULL when no explicit identifier.
select p.paper_id, min(m.journal_key) as journal_key, p.member as publisher_key
from {{ ref('stg_paper') }} p
left join {{ ref('stg_paper_issn') }} i on i.paper_id = p.paper_id
left join {{ ref('int_issn_journal') }} m on m.issn = i.issn
group by p.paper_id, p.member
