{{ config(materialized='incremental', unique_key='timestamp') }}

with cte as (
   select
       timestamp,
       t2.event_type_id,
       description
   from {{ source('game_stats', 'events') }} as t1
   left join {{ ref('event_types') }} as t2 
        on t1.event = t2.event
)

select 
    ROW_NUMBER() OVER (ORDER BY timestamp ASC) AS event_id,
    cte.*
from cte
{% if is_incremental() %}
    where timestamp > (select max(timestamp) from {{ this }})
{% endif %}
order by event_id asc