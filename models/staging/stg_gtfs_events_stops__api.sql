select
    timestamp as event_ts,
    trip_id::varchar as trip_id,
    route_id::varchar as route_id,
    start_time::interval as trip_start_time,
    stop_id::varchar as stop_id,
    stop_sequence::integer as stop_sequence,
    schedule_relationship::varchar as schedule_relationship,
    arrival_delay::integer as arrival_delay_seconds,
    arrival_uncertainty::integer as arrival_uncertainty_seconds,
    departure_delay::integer as departure_delay_seconds,
    departure_uncertainty::integer as departure_uncertainty_seconds,
    ingestion_date::date as ingestion_date,
    strptime(start_date, '%Y%m%d')::date as trip_start_date,
    to_timestamp(arrival_time::bigint) as arrival_ts,
    to_timestamp(departure_time::bigint) as departure_time_ts
from {{ ref('unfolded_gtfs_events_stops__api') }}
