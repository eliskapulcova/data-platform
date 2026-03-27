{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

select
    user_id::int,
        event_type::text,
        duration::int,
        event_time::timestamp
from {{ source('raw', 'raw_user_activity') }}

    {% if is_incremental() %}
where event_time >= (select max(event_time) from {{ this }})
    {% endif %}