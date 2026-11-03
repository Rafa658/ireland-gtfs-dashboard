select
    t.route_id,
    t.trip_id,
    f.event_ts,
    round(f.arrival_delay_seconds / 60.0, 2) as arrival_delay_min,
    round(f.departure_delay_seconds / 60.0, 2) as departure_delay_min
from {{ ref('fct_stop_time_updates') }} as f
left join {{ ref('dim_trips') }} as t
    on f.trip_key = t.trip_key
where 1 = 1
