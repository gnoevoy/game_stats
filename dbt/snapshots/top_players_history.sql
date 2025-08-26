-- History of leaderboard players
-- Used snapshot to track changes in player rankings over time by rank column

{{ config(materialized='snapshot', unique_key='player_id') }}

{% snapshot top_players_history %}

{{
   config(
       target_schema='game_stats_analytics',
       unique_key='player_id',
       strategy='check',
       check_cols=["rank"],
   )
}}

select * 
from {{ ref('players') }}

{% endsnapshot %}