{{ config(
    materialized='table'
) }}

select
    user_id,
    event_type,
    count(*) as event_count,
    sum(duration) as total_duration,
    min(event_time) as first_event,
    max(event_time) as last_event
from {{ ref('stg_user_activity') }}
group by user_id, event_type