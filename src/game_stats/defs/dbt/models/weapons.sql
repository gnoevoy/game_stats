-- Store unique weapons with ID's

with cte as (
    select distinct weapon_name
    from {{ source('game_stats', 'weapons') }}
)

select 
    ROW_NUMBER() OVER (ORDER BY weapon_name ASC) AS weapon_id,
    weapon_name
from cte
order by weapon_name asc