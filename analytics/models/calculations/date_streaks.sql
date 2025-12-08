{# with source as (
    select
        date,
        session_quality,

        case when session_quality = "good" then 1 else 0 end as is_good_session,

    from {{ ref('stg_sessions') }}
),

previous_values as (
    select *,
        lead(date) over (order by date desc) as previous_date,

        lead(is_good_session) over (order by date desc) as previous_good_session,

    from source
),

streak_start as (
    select *,
        case when previous_date is null or date_diff(date, previous_date, day) > 1 then 1
            else 0 end as is_date_streak_start,
        
        case when previous_good_session is null and is_good_session = 1 then 1
            when previous_good_session = 0 and is_good_session = 1 then 1
            else 0 end as is_good_session_streak_start,

    from previous_values
),

streak_groups as (
    select *,
        sum(is_date_streak_start) over (order by date asc) as date_streak_group_id,
        sum(is_good_session_streak_start) over (order by date asc) as good_session_streak_group_id
    from streak_start
),

streak_lengths as (
    select *, 
        count(date_streak_group_id) over (partition by date_streak_group_id) as date_streak_length,
        count(good_session_streak_group_id) over (partition by good_session_streak_group_id) as good_session_streak_length
    from streak_groups
)


select *
from streak_lengths
order by date desc #}