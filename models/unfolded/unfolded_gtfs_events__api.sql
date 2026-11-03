{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append'
    )
}}
with
raw as (
    select *
    from {{ source('raw', 'raw_gtfs_events__api') }}
    {% if is_incremental() %}
    -- ingestion_date prunes Parquet files
        where
            ingestion_date
            > (select max(ingestion_date) from {{ this }})
    {% endif %}
),

unnested as (
    select
        timestamp,
        header::json as header, -- noqa: RF04
        {{ explode_json_array('entity', trip_update_schema()) }} as entity,
        ingestion_date
    from raw
),

unfolded as (
    select
        unnested.timestamp,
        unnested.header,
        entity.id as entity_id,
        entity.trip_update.trip.trip_id,
        entity.trip_update.trip.route_id,
        entity.trip_update.trip.start_date,
        entity.trip_update.trip.start_time,
        entity.trip_update.trip.direction_id,
        entity.trip_update.trip.schedule_relationship
            as trip_schedule_relationship,
        entity.trip_update.timestamp as trip_update_timestamp,
        entity.trip_update.stop_time_update,
        unnested.ingestion_date
    from unnested
)

select * from unfolded
