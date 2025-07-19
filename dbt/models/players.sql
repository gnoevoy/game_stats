-- Main table that shows player statistics
-- Side peaks values are for the last 30 days

select *
from {{ source('game_stats', 'players') }} 
order by player_id asc



