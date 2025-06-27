-- Timestamp table to know at what time the dataset is valid

select
    valid_at as utc_valid_at,
    {{ poland_time("valid_at") }} as poland_valid_at
from {{ source('game_stats', 'timestamp') }}