-- Main table that shows player statistics
-- Added metrics, activity indicator and check if a player has one nickname

with players_with_several_names as (
    select distinct player_id
    from {{ ref('player_names') }}
),

cte as (
    select
        t1.player_id,
        player_name as name,
        case when t2.player_id is null then true else false end as has_one_nickname,
        case when last_month_frags > 0 then true else false end as  has_activity_last_30_days,
        steam_id,
        rank,
        experience,
        frags_per_minute,
        all_time_frags as kills,
        all_time_deaths as deaths,
        all_time_headshots as headshots,
        last_month_frags as last_30_days_kills,
        last_month_deaths as last_30_days_deaths,
        last_month_headshots as last_30_days_headshots,
        ct_side_peaks as last_30_days_ct_side_peaks,
        t_side_peaks as last_30_days_t_side_peaks,
        kill_streak as all_time_kill_streak,
        death_streak as all_time_death_streak,
        suicides as all_time_suicides,
    from {{ source('game_stats', 'players') }} as t1 
    left join players_with_several_names as t2
        on t1.player_id = t2.player_id
)

select *,
    {{ hs_pct("last_30_days_headshots", "last_30_days_kills") }} AS last_30_days_HS_pct,
    {{ hs_pct("headshots", "kills") }} AS HS_pct,
    {{ kdr("last_30_days_kills", "last_30_days_deaths") }} AS last_30_days_KDR,
    {{ kdr("kills", "deaths") }} AS KDR,
from cte
order by rank asc


