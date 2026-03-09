{{ config(
    materialized='table'
) }}

select
    -- driver identity gotten from stg_drivers
    d.driver_id, d.city_id, d.vehicle_id, d.driver_status, d.rating, d.onboarding_date,
    -- Derived: days since onboarding — driver tenure signal
    date_diff(current_date(), d.onboarding_date, day) as days_since_onboarding,

    -- driver level based on lifetime trips Used in mart_driver_leaderboard for cohort segmentation
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

    -- Revenue from driver_metrics
    coalesce(m.total_net_revenue_earned, 0) as total_net_revenue_earned,

    -- Availability from driver_metrics
    coalesce(m.total_online_hours, 0) as total_online_hours,
    m.trips_per_online_hour,

    -- Surge & corporate from driver_metrics
    coalesce(m.extreme_surge_trips, 0) as extreme_surge_trips,
    coalesce(m.corporate_trips, 0) as corporate_trips,

    -- Payment reliability from driver_metrics
    coalesce(m.failed_payment_count, 0) as failed_payment_count,
    coalesce(m.missing_payment_count, 0) as missing_payment_count,
    coalesce(m.duplicate_payment_count, 0) as duplicate_payment_count,
    coalesce(m.invalid_payment_amount_count, 0) as invalid_payment_amount_count

from {{ ref('stg_drivers') }} as d
left join {{ ref('driver_metrics') }} as m
    on d.driver_id = m.driver_id