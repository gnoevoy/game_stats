-- Table to store daily player sessions statistics

{{ config( materialized='incremental', unique_key='date') }}

select
    date,
    experience_change as collected_experience,
    experience,
    kills,
    deaths, 
    headshots,
    time_played_in_minutes, 
    {{ poland_time("timestamp") }} as created_at,
from {{ source('game_stats', 'sessions') }} 

-- Filter incremental runs to the last 30 days avoiding processing all dataset every time
{% if is_incremental() %}
  where date >= (select date_sub(max(date), interval 30 day) from {{ this }})
{% endif %}


