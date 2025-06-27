-- Incremental table that appens new session for my profile 
-- or uptates existting ones if there was some change

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

select *
from cte
{% if is_incremental() %}
    where date > (select max(date) from {{ this }})
{% endif %}
