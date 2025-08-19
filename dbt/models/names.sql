-- Player used nicknames 

select 
    player_id,
    name,
    -- Convert UTC to Poland timezone
    {{ poland_time("last_used") }} as last_used,
from {{ source('game_stats', 'names') }}
order by player_id asc
