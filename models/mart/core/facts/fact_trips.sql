-- Grain: 1 row per trip
--
-- Sources:
--   trips_metrics → all enriched trip data, flags, revenue, duration
--   (dim tables are NOT joined here — fct tables carry FK keys only,
--    joins happen at the mart layer or in the BI tool semantic layer)
--
-- Materialisation: incremental (merge on trip_id)
--   High volume append-style data. Existing trips can change status
--   (e.g. requested → completed) so merge strategy is required over append.
--
-- INCREMENTAL FILTER PLACEMENT:
--   Filter is applied inside the source CTE (not at the end of the query)
--   so that BigQuery filters rows at the earliest possible point.
--   This prevents unnecessary full scans of trips_metrics on every run.
--   Incremental cursor: coalesce(updated_at, requested_at) to catch both
--   new trips and status changes on existing trips.
--
-- DOWNSTREAM USAGE:
--   mart_daily_revenue      → aggregate net_revenue by date, city
--   mart_city_profitability → aggregate by city_id
--   mart_driver_leaderboard → aggregate by driver_id
--   mart_rider_ltv          → aggregate by rider_id
--   mart_fraud_monitoring   → filter on fraud flags
--   mart_payment_reliability → filter on payment flags

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge'
) }}

with source as (

    select *
    from {{ ref('trips_metrics') }}

    {% if is_incremental() %}
        -- Filter at source CTE so all downstream logic only processes new/updated rows
        -- coalesce guards against null updated_at on trips not yet completed
        where coalesce(updated_at, requested_at) > (
            select max(coalesce(updated_at, requested_at))
            from {{ this }}
        )
    {% endif %}

)

select
    -- Primary key
    trip_id,

    -- Foreign keys (dimensional references — joins happen at mart/BI layer)
    driver_id,
    rider_id,
    city_id,

    -- Trip date keys — pre-extracted for dashboard grouping performance
    date(requested_at) as trip_date,
    extract(year from requested_at)  as trip_year,
    extract(month from requested_at) as trip_month,
    extract(dayofweek from requested_at) as trip_day_of_week,

    -- Trip attributes
    status,
    payment_status,
    payment_provider,
    payment_method,
    surge_multiplier,

    -- Measures
    trip_duration_minutes,
    amount,
    fee,
    net_revenue,

    -- Fraud & integrity flags (0/1)
    corporate_trip_flag,
    missing_payment_flag,
    duplicate_payment_flag,
    invalid_payment_amount_flag,
    failed_payment_on_completed_trip_flag,
    extreme_surge_flag,

    -- Timestamps
    requested_at,
    updated_at

from source