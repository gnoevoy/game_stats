-- Incremental table that appends new sessions (day summary basically) and updates existing ones for my profile
-- Dont use conditional logic in incremental model because source data is quite small (30 rows),

{{ config( materialized='incremental', unique_key='date',) }}

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

select *
from cte
