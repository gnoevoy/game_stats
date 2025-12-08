-- Server stats for top 100 current players

-- calculate max and avg stats per player category
with category_stats as (
    select player_category,
        count(*) as player_count,
        max(KDR) as max_KDR,
        max(hs_ratio) as max_hs_ratio,
        max(frags_per_minute) as max_frags_per_minute,
        round(avg(KDR),2) as avg_KDR,
        round(avg(hs_ratio),2 ) as avg_hs_ratio,
        round(avg(frags_per_minute), 2) as avg_frags_per_minute,
    from {{ ref('player_categories') }}
    group by 1 
),

-- calculate 75th percentile stats
percentile_stats as (
    select
        distinct player_category,
        round(percentile_cont(KDR, 0.75) over (partition by player_category), 2) as kdr_p75,
        round(percentile_cont(hs_ratio, 0.75) over (partition by player_category), 2) as hs_ratio_p75,
        round(percentile_cont(frags_per_minute, 0.75) over (partition by player_category), 2) as frags_per_minute_p75,
    from {{ ref('player_categories') }}
)

select
    t1.*, 
    kdr_p75,
    hs_ratio_p75,
    frags_per_minute_p75
from category_stats as t1
left join percentile_stats as t2
   on t1.player_category = t2.player_category
where t1.player_category is not null
