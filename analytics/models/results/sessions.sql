-- Final session table with metrics

select
    -- Dates
    t1.date,
    t1.month,
    t1.month_name,
    t1.week,
    t1.day,
    t1.day_name,

    -- Stats
    t1.kills,
    t1.deaths,
    t1.headshots,
    t1.collected_exp,
    t1.time_played_in_minutes,
    t1.all_time_hs_ratio,
    t1.all_time_KDR,
    t1.all_time_kills_per_minute,
    t1.session_length,
    t1.session_quality,
    t1.avg_hs_ratio_last_5_sessions,
    t1.avg_kdr_last_5_sessions,
    t1.avg_kills_per_minute_last_5_sessions,

    -- Streaks
    t2.date_streak_counter,
    t2.streak_group_id as date_streak_group_id,
    t3.not_short_session_streak_counter,
    t3.streak_group_id as not_short_session_streak_group_id,
    t4.good_session_streak_counter,
    t4.streak_group_id as good_session_streak_group_id,
    t5.not_bad_session_streak_counter,
    t5.streak_group_id as not_bad_session_streak_group_id,
    t6.week_streak_counter,
    t6.streak_group_id as week_streak_group_id,

from {{ ref('sessions_source') }} as t1

-- Join streaks data
left join {{ ref('date_streaks') }} as t2
    on t1.date = t2.date
left join {{ ref('dedication_streaks') }} as t3
    on t1.date = t3.date
left join {{ ref('good_sessions_streak') }} as t4
    on t1.date = t4.date
left join {{ ref('consistency_streaks') }} as t5
    on t1.date = t5.date
left join {{ ref('week_streaks') }} as t6
    on t1.week = t6.week

order by date desc