-- Sessions source table with extra calculations

with source_data as (
    select 
        date,
        kills,
        deaths,
        headshots,
        time_played_in_minutes,

        -- calculated metrics for session_quality
        {{ calculate_ratio('headshots', 'kills', 2) }} as hs_pct,
        {{ calculate_ratio('kills', 'deaths', 2) }} as KDR,

    from {{ source('game_stats_prod', 'sessions') }}
    -- remove empty records
    where time_played_in_minutes > 0
),

-- get my all time metrics
my_stats as (
    select
        {{ calculate_ratio('headshots', 'kills', 2) }} as all_time_hs_pct,
        {{ calculate_ratio('kills', 'deaths', 2) }} as all_time_KDR,
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
        
        case when hs_pct > 0.7 and KDR > 1.7 then 'good'
            when hs_pct < 0.6 or KDR < 1.5 then 'bad'
            else 'average' end as session_quality,

    from source_data as t1
    -- append my all time stats to each row
    cross join my_stats as t2
)

select *
from extra_calculations
order by date desc


