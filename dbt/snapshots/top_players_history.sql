-- Table to track history of top players in the server
-- Join timestamp table to understand when the data was valid

{% snapshot top_players_history %}

-- Use check strategy to append new records only when rank changes

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
cross join {{ ref('timestamp') }}

{% endsnapshot %}