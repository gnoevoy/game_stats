-- Server stats for comparing with my own performance

-- calculate max and avg stats per player category
with category_stats as (
    select player_category,
        max(KDR) as max_KDR,
        max(hs_ratio) as max_hs_ratio,
        max(frags_per_minute) as max_frags_per_minute,
        round(avg(KDR),2) as avg_KDR,
        round(avg(hs_ratio),2 ) as avg_hs_ratio,
        round(avg(frags_per_minute), 2) as avg_frags_per_minute,
    from {{ ref('int_player_categories') }}
    -- filter out uncategorized players
    where player_category is not null
    group by 1 
),

-- pivot category_stats to make columns for each player category
category_pivot as (
    select *
    from category_stats
    pivot (
        max(max_KDR) as max_KDR,
        max(max_hs_ratio) as max_hs_ratio,
        max(max_frags_per_minute) as max_frags_per_minute,
        max(avg_KDR) as avg_KDR,
        max(avg_hs_ratio) as avg_hs_ratio,
        max(avg_frags_per_minute) as avg_frags_per_minute 
        for player_category in ("rifle_soft", "rifle_strong", "sniper_soft", "sniper_strong", "mixed")
    )
),

-- calculate 75th percentile stats for rifle and mixed categories
percentile_stats as (
    select
        distinct player_category,
        round(percentile_cont(KDR, 0.75) over (partition by player_category), 2) as kdr_p75,
        round(percentile_cont(hs_ratio, 0.75) over (partition by player_category), 2) as hs_ratio_p75,
        round(percentile_cont(frags_per_minute, 0.75) over (partition by player_category), 2) as frags_per_minute_p75,

    from {{ ref('int_player_categories') }}
    where player_category in ("rifle_strong", "rifle_soft", "mixed")
),

-- pivot percentile_stats to get wide format
percentile_pivot as (
    select *
    from percentile_stats
    pivot (
        max(kdr_p75) as kdr_p75,
        max(hs_ratio_p75) as hs_ratio_p75,
        max(frags_per_minute_p75) as frags_per_minute_p75
        for player_category in ("rifle_strong", "rifle_soft", "mixed")
    )
),

-- get my all time metrics
my_stats as (
    select
        hs_ratio as all_time_hs_ratio,
        KDR as all_time_KDR,
        frags_per_minute as all_time_kills_per_minute,
    from {{ ref('int_player_categories') }}
    where player_id = 4720
)

-- combine all results into single table
select * 
from my_stats
cross join category_pivot
cross join percentile_pivot
