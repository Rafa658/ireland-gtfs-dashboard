{% macro trip_update_schema() -%}
{
  "id": "VARCHAR",
  "trip_update": {
    "trip": {
      "trip_id": "VARCHAR",
      "route_id": "VARCHAR",
      "start_date": "VARCHAR",
      "start_time": "VARCHAR",
      "direction_id": "VARCHAR",
      "schedule_relationship": "VARCHAR"
    },
    "timestamp": "VARCHAR",
    "stop_time_update": [
      {
        "stop_id": "VARCHAR",
        "stop_sequence": "VARCHAR",
        "schedule_relationship": "VARCHAR",
        "arrival": {
          "delay": "VARCHAR",
          "time": "VARCHAR",
          "uncertainty": "VARCHAR"
        },
        "departure": {
          "delay": "VARCHAR",
          "time": "VARCHAR",
          "uncertainty": "VARCHAR"
        }
      }
    ]
  }
}
{%- endmacro %}
