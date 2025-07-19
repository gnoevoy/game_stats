-- Reusable functions for models 

-- Extract Poland time from UTC timestamp
{% macro poland_time(timestamp) %}
    DATETIME(TIMESTAMP({{ timestamp }}), "Europe/Warsaw")
{% endmacro %}