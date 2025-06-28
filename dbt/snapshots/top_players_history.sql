-- Table to track history of top players in the server
-- Join timestamp table to understand when the data was valid

{% snapshot top_players_history %}

{{
   config(
       target_schema='game_stats_analytics',
       unique_key='player_id',
       strategy='timestamp',
       updated_at='utc_valid_at',
   )
}}

select *
from {{ ref('players') }}
cross join {{ ref('timestamp') }}

{% endsnapshot %}