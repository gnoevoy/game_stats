-- Final session table with metrics

select
    t1.*,
    t2.date_streak_counter,
    t3.not_short_session_streak_counter,
    t4.good_session_streak_counter,
    t5.not_bad_session_streak_counter,
    t6.week_streak_counter,

from {{ ref('sessions_source') }} as t1

-- join streaks data
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