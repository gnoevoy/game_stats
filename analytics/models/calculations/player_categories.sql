-- Leaderboard with player categories based on weapon usage

-- Create bucket for each weapon
with weapon_categories as (
    select *, 
        case when weapon_name in ('ak47', 'aug', 'famas', 'galil', 'm4a1', 'sg552') then 'rifle'
            when weapon_name in ('awp', 'scout') then 'sniper_rifle'
            when weapon_name in ('deagle', 'elite', 'fiveseven', 'glock', 'p228', 'usp') then 'pistol'
            when weapon_name in ('mac10', 'mp5navy', 'p90', 'tmp', 'ump45') then 'smg'
            when weapon_name in ('m3', 'xm1014') then 'shotgun'
            when weapon_name in ('m249') then 'machine_gun'
            when weapon_name in ('hegrenade') then 'grenade'
            when weapon_name in ('knife') then 'melee'
            else 'other' end as weapon_category
    from {{ source('game_stats_prod', 'weapons') }}
),

-- Calculate total kills per weapon category for each player
weapon_usage as (
    select *,
        -- Kills ratio per weapon category
        round(total_kills / sum(total_kills) over (partition by player_id), 2 ) as kills_ratio
    from (
        select 
            player_id, 
            weapon_category,
            sum(kills) as total_kills
        from {{ source('game_stats_prod', 'player_weapons') }} as t1
        -- access weapon categories
        left join weapon_categories as t2
            on t1.weapon_id = t2.weapon_id
        -- filter out to most recent records
        where dbt_valid_to is null
        group by 1, 2 
    )
),

-- Create for each player game-style category
player_categories as(
    select *,
        case when rifle_kills_ratio >= 0.7 then "rifle_strong"
            when sniper_kills_ratio >= 0.7 then "sniper_strong"
            when sniper_kills_ratio >= 0.5 and (sniper_kills_ratio - rifle_kills_ratio) >= 0.2 then "sniper_soft"
            when rifle_kills_ratio >= 0.5 and (rifle_kills_ratio - sniper_kills_ratio) >= 0.2 then "rifle_soft"
            else "mixed" end as player_category
    from (
        -- pivot table to get rifle and sniper kills ratio per player
        select player_id,
            max(case when weapon_category = "rifle" then kills_ratio else null end) as rifle_kills_ratio,
            max(case when weapon_category = "sniper_rifle" then kills_ratio else null end) as sniper_kills_ratio
        from weapon_usage
        group by 1
    )
),

-- Join player categories to leaderboard stats
top_players as (
    select
        t1.player_id,
        player_name,
        rank,
        player_category,
        frags_per_minute,

        -- use macros to calculate ratios
        {{ calculate_ratio('headshots', 'kills', 2) }} as hs_ratio,
        {{ calculate_ratio('kills', 'deaths', 2) }} as KDR,

    from {{ source('game_stats_prod', 'leaderboard') }} as t1
    left join player_categories as t2
        on t1.player_id = t2.player_id
)

select *
from top_players
order by rank asc