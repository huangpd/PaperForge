select paper_id, version_no, content_hash, source_updated_at, valid_from, valid_to, created_at,
       jsonb_array_length(quality_flags) as warning_count
from {{ source('dwd', 'paper_version') }}
