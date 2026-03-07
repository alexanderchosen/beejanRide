{% macro calculate_duration_minutes(start_ts, end_ts) %}
    timestamp_diff({{ end_ts }}, {{ start_ts }}, minute)
{% endmacro %}