-- Table that tracks my events (logs) on the server
-- Used incremental model to append new events

{{ config(materialized='incremental', unique_key=["timestamp_poland", "description"]) }}

with cte as (
   select
        -- Combination of timestamp and event_index gives a unique id for event and allows correctly order dataset
        {{ poland_time("timestamp") }} as timestamp_poland,
        event_index,
        t2.event_type_id,
        description
    from {{ source('game_stats', 'events') }} as t1
    left join {{ ref('event_types') }} as t2 
        on t1.event = t2.event
)

select *
from cte
{% if is_incremental() %}
    where timestamp_poland >= (select max(timestamp_poland) from {{ this }})
{% endif %}