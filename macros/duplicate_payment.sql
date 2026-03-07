{% macro duplicate_payment_flag(trip_id) %}
    count(*) over (partition by {{ trip_id }}) > 1
{% endmacro %}