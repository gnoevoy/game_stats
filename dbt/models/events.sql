-- History of my actions on the server (logs of events)
-- Combination of timestamp and event_index allows correctly order dataset if some events occured at the same time
-- Used incremental model to append new sessions and update existing ones

{{ config(materialized='incremental', unique_key=["timestamp", "description"]) }}

with cte as (
   select
        -- Convert UTC to Poland timezone
        {{ poland_time("timestamp") }} as timestamp,
        event_index,
        t2.event_type_id,
        description
    from {{ source('game_stats', 'events') }} as t1
    -- Join event types to display event id instead of event name
    left join {{ ref('event_types') }} as t2 
        on t1.event = t2.event
)

select *
from cte

-- Filter incremental runs
{% if is_incremental() %}
    where timestamp >= (select max(timestamp) from {{ this }})
{% endif %}