{{ config(materialized='incremental', unique_key='timestamp') }}

with cte as (
   select
        timestamp as utc_timestamp,
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
order by utc_timestamp desc