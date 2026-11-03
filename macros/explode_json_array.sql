{# Parses a varchar/JSON array once into typed structs (per `schema`) and returns one struct per row. #}
{% macro explode_json_array(expression, schema) -%}
    unnest(from_json({{ expression }}, '[{{ schema }}]'))
{%- endmacro %}
