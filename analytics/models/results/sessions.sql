-- Final session table with metrics

select
    -- Dates
    t1.date,

    -- Stats
    t1.kills,
    t1.deaths,
    t1.headshots,
    t1.all_time_hs_pct,
    t1.all_time_KDR,
    t1.session_length,
    t1.session_quality,

    -- Streaks
    t2.good_session_streak_counter,
    t2.streak_group_id as good_session_streak_group_id,

    t3.date_streak_counter,
    t3.streak_group_id as date_streak_group_id,

from {{ ref('sessions_source') }} as t1

-- Join streaks data
left join {{ ref('good_sessions_streak') }} as t2
    on t1.date = t2.date
left join {{ ref('consistency_streaks') }} as t3
    on t1.date = t3.date

order by date desc