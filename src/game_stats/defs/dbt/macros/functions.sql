-- Reusable function to convert UTC to Poland timezone
{% macro poland_time(timestamp) %}
    DATETIME(TIMESTAMP({{ timestamp }}), "Europe/Warsaw")
{% endmacro %}