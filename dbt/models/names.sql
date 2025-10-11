-- Player used nicknames in the server

select 
    player_id,
    name,
    -- Convert UTC to Poland timezone
    {{ poland_time("timestamp") }} as created_at,
    {{ poland_time("last_used") }} as last_used,
from {{ source('game_stats', 'names') }}
order by player_id asc
