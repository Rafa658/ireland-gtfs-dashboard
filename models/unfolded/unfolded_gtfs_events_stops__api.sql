{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}
with
events as (
    select *
    from {{ ref('unfolded_gtfs_events__api') }}
    {% if is_incremental() %}
        where
            ingestion_date
            > (select max(ingestion_date) from {{ this }})
    {% endif %}
),

exploded as (
    select
        timestamp,
        trip_id,
        route_id,
        start_date,
        start_time,
        ingestion_date,
        unnest(stop_time_update) as stu
    from events
),

unfolded as (
    select
        exploded.timestamp,
        exploded.trip_id,
        exploded.route_id,
        exploded.start_date,
        exploded.start_time,
        stu.stop_id,
        stu.stop_sequence,
        stu.schedule_relationship,
        stu.arrival.delay as arrival_delay,
        stu.arrival.time as arrival_time,
        stu.arrival.uncertainty as arrival_uncertainty,
        stu.departure.delay as departure_delay,
        stu.departure.time as departure_time,
        stu.departure.uncertainty as departure_uncertainty,
        exploded.ingestion_date
    from exploded
)

select * from unfolded
