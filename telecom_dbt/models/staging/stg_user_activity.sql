{{ config(
    materialized='incremental',
    unique_key='user_event_id'
) }}

select
    user_id::int,
        event_type::text,
        duration::int,
        event_time::timestamp,
    -- create a unique key per event for incremental loading
        user_id || '_' || event_type || '_' || extract(epoch from event_time)::int as user_event_id
from {{ source('raw', 'raw_user_activity') }}
    {% if is_incremental() %}
where event_time >= (select max(event_time) from {{ this }})
    {% endif %}
order by event_time