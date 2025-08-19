-- Player frags for the last 30 days

select * 
from {{ source('game_stats', 'frags') }}
order by player_id asc, killed_player_id asc