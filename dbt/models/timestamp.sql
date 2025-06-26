select *
from {{ source('game_stats', 'timestamp') }}