-- Calculate ratio of two columns
{% macro calculate_ratio(col_1, col_2, num) %}
    coalesce(round(safe_divide( {{ col_1}} , {{ col_2 }} ), {{num }} ), 1)
{% endmacro %}