{{
    config(
        materialized = 'table'
    )
}}
-- Grain: one trip run (trip_id, route_id, start date and time).
-- trip_id is '' for ADDED trips.
select
    {{
        trip_key(
            'trip_id', 'route_id',
            'trip_start_date', 'trip_start_time'
        )
    }} as trip_key,
    coalesce(trip_id, '') as trip_id,
    route_id,
    trip_start_date,
    trip_start_time,
    any_value(direction_id) as direction_id,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at
from {{ ref('stg_gtfs_events__api') }}
group by 1, 2, 3, 4, 5
