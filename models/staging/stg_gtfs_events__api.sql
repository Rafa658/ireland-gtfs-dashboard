select
    header,
    stop_time_update,
    timestamp as event_ts,
    entity_id::varchar as entity_id,
    trip_id::varchar as trip_id,
    route_id::varchar as route_id,
    -- GTFS times can exceed 24:00:00, so this stays a string
    start_time::interval as trip_start_time,
    direction_id::integer as direction_id,
    trip_schedule_relationship::varchar as trip_schedule_relationship,
    ingestion_date::date as ingestion_date,
    strptime(start_date, '%Y%m%d')::date as trip_start_date,
    to_timestamp(trip_update_timestamp::bigint) as trip_update_ts
from {{ ref('unfolded_gtfs_events__api') }}
