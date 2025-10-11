-- Table that store history of player weapons stats

{% snapshot player_weapons %}

{{ config(unique_key=["player_id", "weapon_id"], strategy='check', check_cols=['kills', "headshots"]) }}

select
    player_id,
    weapon_id, 
    frags as kills,
    headshots,
    {{ poland_time("timestamp") }} as created_at,

from {{ source('game_stats', 'weapons') }} as t1 
-- Join weapons table to display weapon ID instead of weapon name
left join {{ ref('weapons') }} as t2
    on t1.weapon_name = t2.weapon_name

{% endsnapshot %}