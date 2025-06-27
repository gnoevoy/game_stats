-- Store unique weapons with ID's

with cte as (
    select distinct weapon
    from {{ source('game_stats', 'weapons') }}
)

select 
    ROW_NUMBER() OVER (ORDER BY weapon ASC) AS weapon_id,
    weapon
from cte
order by weapon asc