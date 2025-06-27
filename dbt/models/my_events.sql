-- Incremental table that append new events (logs) about my actions on the server.

-- Use combination of columns to uniquely identify each event (several events may appear at the same time).
{{ config(materialized='incremental', unique_key=['utc_timestamp', "description"]) }}

with cte as (
   select
        timestamp as utc_timestamp,
        -- Convert time to the local timezone
        {{ poland_time("timestamp") }} as poland_timestamp,
        t2.event_type_id,
        description
    from {{ source('game_stats', 'events') }} as t1
    left join {{ ref('event_types') }} as t2 
        on t1.event = t2.event
)

select *
from cte
{% if is_incremental() %}
    where utc_timestamp > (select max(utc_timestamp) from {{ this }})
{% endif %}