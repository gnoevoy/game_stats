-- Table that store history of player frags 

{% snapshot player_frags %}

{{ config(unique_key=["player_id", "killed_player_id"], strategy='check', check_cols=['kills', "deaths", "headshots"]) }}

select
    player_id,
    killed_player_id,
    kills,
    deaths,
    headshots,
    {{ poland_time("timestamp") }} as created_at,
from {{ source('game_stats', 'frags') }}

{% endsnapshot %}