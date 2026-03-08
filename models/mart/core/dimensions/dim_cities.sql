-- Grain: 1 row per city
--
-- Source: stg_cities only — no enrichment needed
-- All city attributes are static reference data
--
-- Materialisation: table
--   Low volume (5 cities), full refresh always appropriate,
--   no incremental benefit at this grain.
--
-- DOWNSTREAM USAGE:
--   All mart models join to this for city-level grouping and filtering
--   FK reference target for fct_trips, dim_drivers, dim_riders

{{ config(
    materialized='table'
) }}

select
    city_id, city_name, country, launch_date,
    -- Derived: days since launch — useful for city maturity analysis
    -- in mart_city_profitability
    date_diff(current_date(), launch_date, day)as days_since_launch

from {{ ref('stg_cities')}}