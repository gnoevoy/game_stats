-- Show player actions (for the last 30 days)

with cte as (
    select
        player_id,
        -- split string to extract value
        trim(split(action_name, "-")[safe_offset(0)]) as action,
        value
    from {{ source('game_stats', 'actions') }}
) 

select 
    t1.player_id,
    t2.action_id,
    t1.value
from cte as t1
left join {{ ref('actions') }} as t2
    on t1.action = t2.action
order by t1.player_id asc, t2.action_id asc

