-- Table that store history of player actions 

{% snapshot player_actions %}

{{ config(unique_key=["player_id", "action_id"], strategy='check', check_cols=['value']) }}

select * from {{ ref('actions_cte') }}

{% endsnapshot %}