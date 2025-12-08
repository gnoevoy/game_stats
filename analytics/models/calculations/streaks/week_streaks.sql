-- Calculate consecutive active week streaks (more than 1 session per week)

with source as (
    select
        week,
        count(*) as sessions_num,
        -- flag to check if week is active
        case when count(*) > 1 then 1 else 0 end as is_active

    from {{ ref('sessions_source') }}
    group by 1
),

-- Get previos week and activity flag for each record
previous_values as (
    select *,
        lead(week) over (order by week desc) as previous_week,
        lead(is_active) over (order by week desc) as previous_week_activity,
    from source
),

-- Identify active streak starts
-- Inactive weeks marked as null
streak_start as (
    select *,
        case when is_active = 0 then null
            when previous_week is not null and is_active = 1 and (week - previous_week) = 1 and  previous_week_activity = 1 then 0
            else 1 end as is_start,

    from previous_values
),

-- Assign group ids to streaks
streak_groups as (
    select *,
        case when is_start is null then null
            else sum(is_start) over (order by week asc) end as streak_group_id,
    from streak_start
),

-- Calculate streak lengths 
streak_lengths as (
    select *, 
        case when is_start is null then null
            else row_number() over (partition by streak_group_id order by week asc) end as week_streak_counter
    from streak_groups
)

select *
from streak_lengths
order by week desc