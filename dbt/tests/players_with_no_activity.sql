-- Check if a player really dont have activity for the last 30 days
-- Most of players in top 100 usually often plays in the server

with players_with_no_activity_for_the_last_30_days as (
    select *
    from {{ ref('players') }}
    where has_activity_last_30_days is false
)

select player_id
from {{ ref('player_frags') }}
where player_id in (select player_id from players_with_no_activity_for_the_last_30_days)