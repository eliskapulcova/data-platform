{{ config(
    materialized='table'
) }}

select
    user_id,
    sum(total_duration) as total_duration_all,
    sum(event_count) as total_events
from {{ ref('int_user_activity') }}
group by user_id