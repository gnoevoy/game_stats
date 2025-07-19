-- Table to track history of top players in the server by rank

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
cross join (select valid_at_poland_time from {{ ref('timestamp') }})

{% endsnapshot %}