{{
    config(
        materialized='view'
    )
}}
select
    route_id,
    round(avg(arrival_delay_min) / 60.0, 2) as avg_arrival_delay_min,
    round(avg(departure_delay_min) / 60.0, 2) as avg_departure_delay_min,
    count(*) as stop_events
from {{ ref('trip_delays') }}
where 1 = 1
group by route_id
order by avg_arrival_delay_min desc
