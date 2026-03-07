{% macro calculate_net_revenue(amount_col, fee_col) %}
    (coalesce({{ amount_col }}, 0) - coalesce({{ fee_col }}, 0))
{% endmacro %}