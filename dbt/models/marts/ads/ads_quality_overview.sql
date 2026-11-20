{{ config(materialized='view') }}
-- Grain: rule. Latest recorded result per rule across all engines (pandera / sql / python-validator / dbt).
select distinct on (r.rule_id)
       r.rule_id, r.rule_version, q.dimension, q.severity, q.dataset, q.description, q.engine,
       r.total_rows, r.failed_rows, r.passed,
       {{ rate('(r.total_rows - r.failed_rows)', 'r.total_rows') }} as pass_rate,
       r.run_at, r.run_id
from {{ source('meta', 'quality_result') }} r
join {{ source('meta', 'quality_rule') }} q on q.rule_id = r.rule_id and q.version = r.rule_version
order by r.rule_id, r.run_at desc
