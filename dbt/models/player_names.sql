-- Table that shows used names by a player
-- Dont contain users who only used one nickname in the server

with cte as (
    select *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY last_used DESC) AS rank
    from {{ source('game_stats', 'names') }}
)

select 
    player_id,
    name,
    last_used as utc_last_used,
    {{ poland_time("last_used") }} as poland_last_used,
    case when rank = 1 then true else false end as is_last_used_name
from cte
order by player_id asc, rank asc
