{{ config(
    materialized='table'
) }}

select
    user_id::int,
        event_type::text,
        duration::int,
        event_time::timestamp
from {{ source('raw', 'raw_user_activity') }}