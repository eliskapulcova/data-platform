{{ config(
    materialized='incremental',
    unique_key='user_event_key'
) }}

with staged as (

    select
    user_id::int,
    event_type::text,
    duration::int,
    event_time::timestamp,
    -- create composite key for incremental uniqueness
    user_id || '_' || event_type as user_event_key
      from {{ ref('stg_user_activity') }}

)

select *
from staged

    {% if is_incremental() %}
where user_event_key not in (select user_event_key from {{ this }})
    {% endif %}