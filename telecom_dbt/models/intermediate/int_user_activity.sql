{{ config(
    materialized='incremental',
    unique_key='user_event_id',
    post_hook=[
  "CREATE INDEX IF NOT EXISTS idx_int_user_event_id ON {{ this }} (user_event_id)"
]
) }}

with staged as (
    select *
    from {{ ref('stg_user_activity') }}
),

enriched as (
    select
        user_id,
        event_type,
        duration,
        event_time,

        -- deterministic unique key
        md5(
            user_id || event_type || event_time::text
        ) as user_event_id

    from staged
),

deduplicated as (
    select *
    from enriched e
    where not exists (
        select 1
        from {{ this }} t
        where t.user_event_id = e.user_event_id
    )
)

select *
from deduplicated

    {% if is_incremental() %}
-- handle late arriving data (reprocess small window)
where event_time > (
    select coalesce(max(event_time), '1900-01-01'::timestamp) - interval '5 minutes'
    from {{ this }}
    )
    {% endif %}