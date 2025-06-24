with source as (
    select
        distinct
        trim(split(action_name, "-")[safe_offset(0)]) as action,
        trim(split(action_name, "-")[safe_offset(1)]) as description,
    from {{ source('game_stats', 'actions') }}
    order by action asc
) 

select 
    ROW_NUMBER() OVER (ORDER BY action ASC) AS action_id,
    action,
    description
from source
order by action asc