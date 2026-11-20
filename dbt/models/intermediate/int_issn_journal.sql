-- Deterministic journal identity from explicit identifiers only (never from names):
-- an ISSN maps to the smallest ISSN it is co-listed with on any paper (print/electronic pairs collapse).
-- NOTE: one-hop, not a transitive closure; documented limitation.
select a.issn, min(b.issn) as journal_key
from {{ ref('stg_paper_issn') }} a
join {{ ref('stg_paper_issn') }} b using (paper_id)
group by a.issn
