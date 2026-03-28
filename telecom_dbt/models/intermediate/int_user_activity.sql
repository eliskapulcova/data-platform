{{ config(
    materialized='incremental',
    unique_key='user_event_id'
) }}

with staged as (
    select
        user_id::int,
        event_type::text,
        duration::int,
        event_time::timestamp,
        user_event_id
    from {{ ref('stg_user_activity') }}
)

select *
from staged
    {% if is_incremental() %}
where user_event_id not in (select user_event_id from {{ this }})
    {% endif %}
order by event_time