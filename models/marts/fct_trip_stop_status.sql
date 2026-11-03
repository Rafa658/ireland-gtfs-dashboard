{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['trip_key', 'stop_sequence']
    )
}}
-- Accumulating snapshot fact.
-- Grain: one row per trip run and stop, holding the latest prediction seen
-- (a proxy for actual delay). Trip attributes live in dim_trips.
with events as (
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
        departure_delay_seconds,
        departure_time_ts,
        ingestion_date
    from {{ ref('stg_gtfs_events_stops__api') }}
)

select
    trip_key,
    stop_sequence,
    arg_max(stop_id, event_ts) as stop_id,
    arg_max(schedule_relationship, event_ts) as schedule_relationship,
    arg_max(arrival_delay_seconds, event_ts) as arrival_delay_seconds,
    arg_max(arrival_ts, event_ts) as arrival_ts,
    arg_max(departure_delay_seconds, event_ts) as departure_delay_seconds,
    arg_max(departure_time_ts, event_ts) as departure_time_ts,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at,
    count(*) as prediction_count
from events
{% if is_incremental() %}
    -- re-aggregate full history of trips touched by recent snapshots
    where trip_key in (
        select recent.trip_key
        from events as recent
        where
            recent.ingestion_date
            >= (select max(t.last_seen_at)::date from {{ this }} as t)
    )
{% endif %}
group by trip_key, stop_sequence
