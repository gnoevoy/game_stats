-- Current leaderboard with players stats

select
    player_id,
    player_name,
    rank, 
    experience, 
    deaths,
    kills,
    headshots, 
    frags_per_minute, 
    last_30_days_kills, 
    last_30_days_deaths,
    last_30_days_headshots,
    CT_side_peaks as ct_side_peaks_for_last_30_days,
    T_side_peaks as t_side_peaks_for_last_30_days,
from {{ source('game_stats', 'players') }} 
order by rank asc



