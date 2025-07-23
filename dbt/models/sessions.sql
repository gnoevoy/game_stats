-- Show my sessions (one session = day summary) on the server  
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
