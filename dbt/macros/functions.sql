{# Calculate KDR metric #}
{% macro kdr(kills, deaths) %}
    round(SAFE_DIVIDE({{ kills }}, {{ deaths }}), 2) 
{% endmacro %}


{# Calculate HS percentage #}
{% macro hs_pct(headshots, kills) %}
    round(SAFE_DIVIDE({{ headshots }}, {{ kills }}) * 100, 2) 
{% endmacro %}


{# Get Poland time from utc #}
{% macro poland_time(timestamp) %}
    DATETIME(TIMESTAMP({{ timestamp }}), "Europe/Warsaw")
{% endmacro %}