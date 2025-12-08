-- Sessions source table with extra calculations

with source_data as (
    select 
        -- date values
        date,
        extract(month from date) as month,
        format_datetime("%B", date) as month_name,
        extract(week from date) as week,
        extract(dayofweek from date) as day,
        format_date('%A', date) AS day_name,

        kills,
        deaths,
        headshots,
        collected_experience as collected_exp,
        time_played_in_minutes,

        -- calculated metrics using macros
        {{ calculate_ratio('headshots', 'kills', 2) }} as hs_ratio,
        {{ calculate_ratio('kills', 'deaths', 2) }} as KDR,
        {{ calculate_ratio('collected_experience', 'time_played_in_minutes', 2) }} as exp_per_minute,
        {{ calculate_ratio('kills', 'time_played_in_minutes', 2) }} as kills_per_minute,    

    from {{ source('game_stats_prod', 'sessions') }}
    -- remove empty records
    where time_played_in_minutes > 0
),

-- get my all time metrics
my_stats as (
    select
        {{ calculate_ratio('headshots', 'kills', 2) }} as all_time_hs_ratio,
        {{ calculate_ratio('kills', 'deaths', 2) }} as all_time_KDR,
        frags_per_minute as all_time_kills_per_minute,
    from {{ source('game_stats_prod', 'leaderboard_history') }}
    where player_id = 4720 and dbt_valid_to is null
),

extra_calculations as (
    select *,
        -- buckets for session length and performance
        case when time_played_in_minutes <= 30 then "1-30 min"
            when time_played_in_minutes <= 60 then "31-60 min"
            when time_played_in_minutes <= 120 then "61-120 min"
            else "120+ min" end as session_length,
        
        case when hs_ratio > 0.7 and KDR > 1.7 then 'good'
            when hs_ratio < 0.6 or KDR < 1.5 then 'bad'
            else 'average' end as session_quality,

        -- net comparison to my all time stats
        round((hs_ratio - all_time_hs_ratio) / all_time_hs_ratio * 100 , 2) as hs_ratio_vs_all_time_pct,
        round((KDR - all_time_KDR) / all_time_KDR * 100 , 2) as kdr_vs_all_time_pct,
        round((kills_per_minute - all_time_kills_per_minute) / all_time_kills_per_minute * 100 , 2) as kills_per_minute_vs_all_time_pct,

        -- rolling averages over last 5 sessions
        round(avg(hs_ratio) over (order by date rows between 4 preceding and current row), 2) as avg_hs_ratio_last_5_sessions,
        round(avg(kdr) over (order by date rows between 4 preceding and current row), 2) as avg_kdr_last_5_sessions,
        round(avg(kills_per_minute) over (order by date rows between 4 preceding and current row), 2) as avg_kills_per_minute_last_5_sessions,

    from source_data as t1
    -- append my all time stats to each row
    cross join my_stats as t2
)

select *
from extra_calculations
order by date desc


