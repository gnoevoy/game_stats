-- Show used nicknames for player
-- Dont contain users who only used one nickname in the server

select 
    player_id,
    name,
    {{ poland_time("last_used") }} as last_used_poland_time,
from {{ source('game_stats', 'names') }}
order by player_id asc
