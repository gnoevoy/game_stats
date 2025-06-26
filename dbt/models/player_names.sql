with cte as (
    select *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY last_used DESC) AS rank
    from {{ source('game_stats', 'names') }}
)

select 
    player_id,
    name,
    last_used,
    case when rank = 1 then true else false end as is_last_used_name
from cte
order by player_id asc, rank asc
