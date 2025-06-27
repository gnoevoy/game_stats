-- Check if a player that has one nickname is not appear in the player_names table
-- That table only contains players with multiple nicknames

with cte as (
    select player_id
    from {{ ref('players') }}
    where has_one_nickname is true
)

select distinct player_id
from {{ ref('player_names') }}
where player_id in (select player_id from cte)