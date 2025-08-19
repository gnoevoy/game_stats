-- Sessions summary for my profile (session = day summary)
-- Used incremental model to append new sessions and update existing ones

{{ config( materialized='incremental', unique_key='date') }}

select
   date,
   experience_change as collected_experience,
   experience,
   kills,
   deaths, 
   headshots,
   time_played_in_minutes
from {{ source('game_stats', 'sessions') }} 

-- Filter down to the last 30 days on incremental runs to avoid processing all dataset every time
{% if is_incremental() %}
  where date >= (select date_sub(max(date), interval 30 day) from {{ this }})
{% endif %}


