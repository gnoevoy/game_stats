-- Table that store history of player actions 

{% snapshot player_actions %}

{{ config( unique_key='player_id', strategy='check', check_cols=["action_id", 'value']) }}

with cte as (
    select
        player_id,
        -- Split string column to extract value
        trim(split(action_name, "-")[safe_offset(0)]) as action,
        value,
        {{ poland_time("timestamp") }} as created_at,
    from {{ source('game_stats', 'actions') }}
) 

select 
    t1.player_id,
    t2.action_id,
    t1.value,
    t1.created_at,
from cte as t1
-- Join actions table to display action ID instead of action name
left join {{ ref('actions') }} AS t2
    on t1.action = t2.action
order by t1.player_id asc, t2.action_id asc

{% endsnapshot %}