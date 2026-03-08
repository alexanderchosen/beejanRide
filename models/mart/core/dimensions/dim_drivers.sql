-- Grain: 1 row per driver (current state)
--
-- Sources:
--   stg_drivers     → identity, status, vehicle, rating, onboarding date
--   driver_metrics  → aggregated performance metrics for BI reporting
--
-- This is a Type 1 SCD — always reflects current driver state.
-- Historical state changes (status, vehicle, rating) are tracked
-- separately via the drivers_snapshot SCD Type 2 snapshot.
--
-- Materialisation: table
--   Full driver population always needed for FK integrity.
--   Dimension tables must never be incremental — partial loads
--   would break joins in fct_trips for drivers with no recent trips.
--
-- DOWNSTREAM USAGE:
--   fct_trips          → FK join on driver_id
--   mart_driver_leaderboard → rankings on performance columns
--   mart_city_profitability → city_id grouping
--   mart_fraud_monitoring   → extreme_surge_trips, cancellation signals

{{ config(
    materialized='table'
) }}

select
    -- Identity (from stg_drivers)
    d.driver_id, d.city_id, d.vehicle_id, d.driver_status, d.rating, d.onboarding_date,

    -- Derived: days since onboarding — driver tenure signal
    date_diff(current_date(), d.onboarding_date, day) as days_since_onboarding,

    -- Derived: driver tier based on lifetime trips
    -- Used in mart_driver_leaderboard for cohort segmentation
    case
        when coalesce(m.driver_lifetime_trips, 0) = 0  then 'Level 0'
        when coalesce(m.driver_lifetime_trips, 0) <= 1  then 'Level 1'
        when coalesce(m.driver_lifetime_trips, 0) <= 3  then 'Level 2'
        else 'Level 3'
    end as driver_level,

    -- Trip volume (from driver_metrics)
    coalesce(m.driver_lifetime_trips, 0) as driver_lifetime_trips,
    coalesce(m.cancelled_trips, 0) as cancelled_trips,
    coalesce(m.no_show_trips, 0) as no_show_trips,
    coalesce(m.total_trip_attempts, 0) as total_trip_attempts,

    -- Revenue (from driver_metrics)
    coalesce(m.total_net_revenue_earned, 0) as total_net_revenue_earned,
    m.avg_net_revenue_per_trip,                         -- keep null: no trips yet

    -- Availability (from driver_metrics)
    coalesce(m.total_online_hours, 0) as total_online_hours,
    m.trips_per_online_hour,                            -- keep null: never logged on

    -- Performance rates (from driver_metrics)
    m.completion_rate_pct,                              -- keep null: no attempts yet
    m.cancellation_rate_pct,                            -- keep null: no attempts yet
    m.avg_surge_multiplier,                             -- keep null: no trips yet

    -- Surge & corporate (from driver_metrics)
    coalesce(m.extreme_surge_trips, 0) as extreme_surge_trips,
    coalesce(m.corporate_trips, 0) as corporate_trips,

    -- Payment reliability (from driver_metrics)
    coalesce(m.failed_payment_count, 0) as failed_payment_count,
    coalesce(m.missing_payment_count, 0) as missing_payment_count,
    coalesce(m.duplicate_payment_count, 0) as duplicate_payment_count,
    coalesce(m.invalid_payment_amount_count, 0) as invalid_payment_amount_count

from {{ ref('stg_drivers') }} as d
left join {{ ref('driver_metrics') }} as m
    on d.driver_id = m.driver_id