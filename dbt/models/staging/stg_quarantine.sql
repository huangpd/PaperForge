select id, source, source_id, rule_id, rule_version, status, created_at, replayed_at
from {{ source('meta', 'quarantine_record') }}
