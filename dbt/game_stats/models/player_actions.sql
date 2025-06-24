with source as (
    select
        player_id,
        trim(split(action_name, "-")[safe_offset(0)]) as action,
        value
    from {{ source('game_stats', 'actions') }}
    order by action asc
) 

select 
    t1.player_id,
    t2.action_id,
    t1.value
from source as t1
left join {{ ref('actions') }} as t2
on t1.action = t2.action
order by t1.player_id asc, t2.action_id asc

