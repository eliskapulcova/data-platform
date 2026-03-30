{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_mrt_user_metrics_date_user ON {{ this }} (event_date, user_id)"
    ]
) }}

with base as (
    select *
    from {{ ref('int_user_activity') }}
),

     aggregated as (
         select
             user_id,
             date_trunc('day', event_time) as event_date,

             -- core metrics
             count(*) as total_events,
             count(distinct event_type) as distinct_event_types,
             sum(duration) as total_duration,
             avg(duration) as avg_duration,

             -- event type breakdown 👇
             count(*) filter (where event_type = 'call') as call_events,
             count(*) filter (where event_type = 'sms') as sms_events,
             count(*) filter (where event_type = 'internet') as internet_events,

        -- time boundaries
             min(event_time) as first_event,
             max(event_time) as last_event

         from base
         group by user_id, date_trunc('day', event_time)
     )

select *
from aggregated