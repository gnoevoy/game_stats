-- Table to store my logs on the server
-- Used incremental model to append only new events

{{ config(materialized='incremental')}}

with cte as (
   select
        {{ poland_time("timestamp") }} as created_at,
        event_index,
        t2.event_type_id,
        description, 

    from {{ source('game_stats', 'events') }} as t1
    -- Join event_types table to display event_id instead of event_name
    left join {{ ref('event_types') }} as t2 
        on t1.event = t2.event
)

select *
from cte
-- Filter incremental runs
{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{% endif %}