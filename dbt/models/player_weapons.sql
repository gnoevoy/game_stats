-- Display player weapon statistics (for the last 30 days)

with cte as (
    select
        player_id,
        weapon_id, 
        frags as kills,
        headshots,
    from {{ source('game_stats', 'weapons') }} as t1 
    left join {{ ref('weapons') }} as t2
        on t1.weapon_name = t2.weapon_name
)

select *
from cte
order by player_id asc, weapon_id asc

