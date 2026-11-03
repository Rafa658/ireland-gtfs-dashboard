{% macro trip_key(trip_id, route_id, start_date, start_time) %}
    md5(concat_ws('|', coalesce({{ trip_id }}, ''), {{ route_id }}, {{ start_date }}, {{ start_time }}))
{% endmacro %}
