-- Table that shows individual weapon stats by a player for the last 30 days

with cte as (
    select
        player_id,
        weapon_id, 
        frags as kills,
        headshots,
    from {{ source('game_stats', 'weapons') }} as t1 
    left join {{ ref('weapons') }} as t2
        on t1.weapon = t2.weapon
)

select *, {{ hs_pct("headshots", "kills") }} AS HS_pct
from cte
order by player_id asc, weapon_id asc

