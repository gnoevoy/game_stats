-- Timestamp value to know at what time the dataset is valid

select *, {{ poland_time("valid_at") }} as valid_at_poland_time
from {{ source('game_stats', 'timestamp') }}