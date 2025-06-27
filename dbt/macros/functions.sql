-- Reusable functions for models 


-- Calculate KDR (Kill/Death Ratio) metric
{% macro kdr(kills, deaths) %}
    round(SAFE_DIVIDE({{ kills }}, {{ deaths }}), 2) 
{% endmacro %}


-- Calculate Headshot percentage
{% macro hs_pct(headshots, kills) %}
    round(SAFE_DIVIDE({{ headshots }}, {{ kills }}) * 100, 2) 
{% endmacro %}


-- Extract Poland time from UTC timestamp
{% macro poland_time(timestamp) %}
    DATETIME(TIMESTAMP({{ timestamp }}), "Europe/Warsaw")
{% endmacro %}