{{
    config(
        materialized='incremental',
        unique_key=['player_id'],
    )
}}


select *
from {{ ref('example_file') }}
{# {% if is_incremental() %}
    where player_id > (select max(player_id) from {{ this }})
{% endif %} #}