{{ config(
    materialized='incremental'
) }}

select
    user_id::int as user_id,
        event_type::text as event_type,
        duration::int as duration,
        event_time::timestamp as event_time
from {{ source('raw', 'raw_user_activity') }}

    {% if is_incremental() %}
where event_time > (
    select coalesce(max(event_time), '1900-01-01'::timestamp)
    from {{ this }}
    )
    {% endif %}