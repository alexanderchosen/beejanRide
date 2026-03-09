
{{ config(
    materialized='table'
) }}

select
    city_id, city_name, country, launch_date,
    date_diff(current_date(), launch_date, day)as days_since_launch
from {{ ref('stg_cities')}}