-- Calculate consecutive date streaks

with source as (
    select date
    from {{ ref('sessions_source') }}
),

-- Get previos date for each record
previous_values as (
    select *,
        lead(date) over (order by date desc) as previous_date,
    from source
),

-- Identify streak starts
streak_start as (
    select *,
        case when previous_date is null or date_diff(date, previous_date, day) > 1 then 1
            else 0 end as is_start,
    from previous_values
),

-- Assign group ids to streaks
streak_groups as (
    select *,
        sum(is_start) over (order by date asc) as streak_group_id,
    from streak_start
),

-- Calculate streak lengths 
streak_lengths as (
    select *, 
        row_number() over (partition by streak_group_id order by date asc) as date_streak_counter
    from streak_groups
)


select *
from streak_lengths
order by date desc