-- Calculate consecutive good sessions

with source as (
    select
        date,
        case when session_quality = "good" then 1 else 0 end as is_good_session,

    from {{ ref('sessions_source') }}
),

-- Get previous session quality for each record
previous_values as (
    select *,
        lead(is_good_session) over (order by date desc) as previous_value,
    from source
),

-- Identify good sessions streak starts
streak_start as (
    select *,
        case when is_good_session = 0 then null
            when previous_value is not null and is_good_session = 1 and previous_value = 1 then 0
            else 1 end as is_start,

    from previous_values
),

-- Assign group ids to streaks
streak_groups as (
    select *,
        case when is_start is null then null
            else sum(is_start) over (order by date asc) end as streak_group_id,
    from streak_start
),

-- Calculate streak lengths 
streak_lengths as (
    select *, 
        case when is_start is null then null
            else row_number() over (partition by streak_group_id order by date asc) end as good_session_streak_counter
    from streak_groups
)

select *
from streak_lengths
order by date desc