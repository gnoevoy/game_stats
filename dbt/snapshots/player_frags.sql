-- Table that store history of player frags 

{% snapshot player_actions %}

{{ config( unique_key='player_id', strategy='check', check_cols=["action_id", 'value']) }}



{% endsnapshot %}