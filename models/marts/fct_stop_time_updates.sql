{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'ingestion_date'
    )
}}
-- Grain: one prediction for a trip at a stop in a feed snapshot.
-- Trip attributes live in dim_trips (join on trip_key).
select
    event_ts,
    {{
        trip_key(
            'trip_id', 'route_id',
            'trip_start_date', 'trip_start_time'
        )
    }} as trip_key,
    stop_id,
    stop_sequence,
    schedule_relationship,
    arrival_delay_seconds,
    arrival_ts,
    arrival_uncertainty_seconds,
    departure_delay_seconds,
    departure_time_ts,
    departure_uncertainty_seconds,
    ingestion_date
from {{ ref('stg_gtfs_events_stops__api') }}
{% if is_incremental() %}
    where
        ingestion_date
        > (select max(ingestion_date) from {{ this }})
{% endif %}
