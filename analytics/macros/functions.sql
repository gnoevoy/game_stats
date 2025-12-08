-- Reusable functions to calculate column values

-- Calculate ratio of two columns
{% macro calculate_ratio(col_1, col_2, num) %}
    coalesce(round(safe_divide( {{ col_1}} , {{ col_2 }} ), {{num }} ), 1)
{% endmacro %}

-- Calculate pct difference between two columns
{% macro pct_difference(col_1, col_2) %}
    round( ({{col_1}} - {{col_2}}) / {{col_2}}, 2)
{% endmacro %}

-- Rolling averages for 5 last records
{% macro rolling_avg(col_1, date) %}
    round(avg({{col_1}}) over (order by {{date}} rows between 4 preceding and current row), 2)
{% endmacro %}