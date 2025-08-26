-- Store unique actions with ID's

with cte as (
    select
        distinct
        -- Split string column and extract data
        trim(split(action_name, "-")[safe_offset(0)]) as action,
        trim(split(action_name, "-")[safe_offset(1)]) as description,
    from {{ source('game_stats', 'actions') }}
) 

select 
    -- Create incremental ID for each action
    ROW_NUMBER() OVER (ORDER BY action ASC) AS action_id,
    action,
    description
from cte
order by action asc