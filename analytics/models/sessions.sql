with source_data as (
    select 
        date,
        extract(month from date) as month,
        extract(week from date) as week,
        extract(dayofweek from date) as day,

        kills,
        deaths,
        headshots,
        coalesce(round(safe_divide(headshots, kills),2 ), 1) as hs_ratio,
        coalesce(round(safe_divide(kills, deaths),2 ), 1) as KDR,

        collected_experience as collected_exp,
        time_played_in_minutes,
        round(collected_experience / time_played_in_minutes, 2) as exp_per_minute,
        coalesce(round(safe_divide(kills, time_played_in_minutes),2 ), 0) as kills_per_minute,

    from {{ source('game_stats_prod', 'sessions') }}
    where time_played_in_minutes > 0
),

all_time_stats as (
    select
        coalesce(round(safe_divide(headshots, kills),2 ), 1) as all_time_hs_ratio,
        coalesce(round(safe_divide(kills, deaths),2 ), 1) as all_time_KDR,
        frags_per_minute as all_time_kills_per_minute,
    from {{ source('game_stats_prod', 'leaderboard_history') }}
    where player_id = 4720 and dbt_valid_to is null
)


select *,
    case when time_played_in_minutes <= 30 then "0-30 min"
        when time_played_in_minutes <= 60 then "31-60 min"
        when time_played_in_minutes <= 120 then "61-120 min"
        else "120+ min" end as session_length,
    
    case when hs_ratio > 0.7 and KDR > 1.7 then 'good'
        when hs_ratio < 0.6 or KDR < 1.5 then 'bad'
        else 'average' end as session_quality,

from source_data
cross join all_time_stats
order by date desc


