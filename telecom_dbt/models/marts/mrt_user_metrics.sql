{{ config(
    materialized='table'
) }}

select
    user_id,
    count(*) as total_events,
    count(distinct event_type) as distinct_event_types,
    sum(duration) as total_duration,
    min(event_time) as first_event,
    max(event_time) as last_event
from {{ ref('int_user_activity') }}
group by user_id