-- Dispaly stats for each player category

select *
from {{ ref('server_stats') }}