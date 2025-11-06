-- Table that tracks changes in player ranks over time

{% snapshot leaderboard_history %}

{{ config( unique_key='player_id', strategy='check', check_cols=["rank"]) }}

select * 
from {{ ref('leaderboard') }}

{% endsnapshot %}