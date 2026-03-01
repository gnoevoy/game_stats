-- Players categories with ranking and stats

with players_ranking as (
    select *, 
        -- Equal distribution within each category
        ntile(5) over (partition by player_category order by rnk_within_category asc) as player_level,
    from (
        -- Creaate ranking withing each player group
        select *, row_number() over (partition by player_category order by rank asc) as rnk_within_category
        from {{ ref('leaderboard_with_categories') }}
    )
),

-- get my all time stats
my_stats as (
    select
        {{ calculate_ratio('headshots', 'kills', 2) }} as my_all_time_hs_pct,
        {{ calculate_ratio('kills', 'deaths', 2) }} as my_all_time_KDR,
        frags_per_minute as my_all_time_frags_per_minute,    

    from {{ source('game_stats_prod', 'leaderboard_history') }}
    where player_id = 4720 and dbt_valid_to is null
),

players_categories as (
    select 
        player_category,
        -- Player buckets 
        case when player_level = 1 then "tier 1 (top 20%)"
            when player_level = 2 then "tier 2 (20-40%)"
            when player_level = 3 then "tier 3 (40-60%)"
            when player_level = 4 then "tier 4 (60-80%)"
            else "tier 5 (bottom 20%)"
        end as player_bucket,

        -- Stats
        count(distinct player_id) as players_num,
        round(avg(hs_pct),2 ) as avg_hs_pct,
        round(avg(KDR),2) as avg_KDR,
        round(avg(frags_per_minute),2) as avg_frags_per_minute,

    from players_ranking
    where player_category is not null
    group by 1,2 
)

select t1.*,
    round((my_all_time_hs_pct - avg_hs_pct) / avg_hs_pct, 2) as hs_pct_difference,
    round((my_all_time_KDR - avg_KDR) / avg_KDR, 2) as kdr_difference,
    round((my_all_time_frags_per_minute - avg_frags_per_minute) / avg_frags_per_minute, 2) as frags_per_minute_difference,

from players_categories as t1
-- append my all time stats to each row
cross join my_stats as t2
order by player_category asc, player_bucket asc