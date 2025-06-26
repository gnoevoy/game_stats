{{ config(materialized='incremental', unique_key='date') }}

with cte as (
   select
        date,
        experience_change as collected_experience,
        experience,
        frags as kills,
        deaths, 
        headshots,
        {{ kdr("frags", "deaths") }} as KDR,
        {{ hs_pct("headshots", "frags") }} as HS_pct,
        time_played_in_minutes
   from {{ source('game_stats', 'sessions') }} 
)

select 
    ROW_NUMBER() OVER (ORDER BY date ASC) AS session_id,
    cte.*
from cte
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
order by session_id asc
