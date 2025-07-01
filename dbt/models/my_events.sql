-- Table that containts logs about my actions on the server (different events like connections, kill, deaths ...)
-- Also incrementaly append new events by combined id, since some events may occur at the same time.

{{ config(materialized='incremental', unique_key=['utc_timestamp', "description"]) }}

with cte as (
   select
        timestamp as utc_timestamp,
        -- Convert time to the local timezone
        {{ poland_time("timestamp") }} as poland_timestamp,

        -- This columns allows to sort events correctly when some occurs at the sama time, the lowest value will be the last event 
        event_seq_num, 

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