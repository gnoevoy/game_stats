select *, 
    {{ kdr("kills", "deaths") }} AS KDR, 
    {{ hs_pct("headshots", "kills") }} AS HS_pct
from {{ source('game_stats', 'frags') }}
order by player_id asc, killed_player_id asc