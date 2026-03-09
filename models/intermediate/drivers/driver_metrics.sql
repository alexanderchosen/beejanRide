-- Grain: 1 row per driver, including drivers with zero trips

-- NULL STRATEGY:
--   a driver with no trips has 0 trips, £0 earned, therefore I need to coalesce to 0 to avoid equating a driver with no trips to a driver wuth poor records
--   Also, for the rating, I need to keep null data for new driver with no rating yet, because if it is coalesced to 0, it will mean the driver already has a poor performance rating immediately after joining


{{ config(
    materialized='table'
)
}}

-- So, I need to collect data from my stg_drivers data which will help fulfil the business and logic requirements
with driver_base as (
    select
        driver_id, city_id, vehicle_id, driver_status, rating, onboarding_date
    from {{ ref('stg_drivers') }}
),

-- I need to use trips_metrics model to get vital data such as net_revenue for drivers, trip status per driver, missin payment per driver, failed payment attempts
-- total number of trips per driver, total number of cancelled trips, no_show trips per driver
-- a driver with no completed trips has earned £0, therefore i should coalesce to 0
trip_metrics as (
    select driver_id,
        -- I used count to calculate the volume of trips per driver id
        count(case when status = 'completed' then 1 end) as driver_lifetime_trips,
        count(case when status = 'cancelled' then 1 end ) as cancelled_trips,
        count( case when status = 'no_show' then 1 end) as no_show_trips,
        count(*) as total_trip_attempts,
-- I used net_revenue from trips_metrics (amount - fee via macro) to calculate the total_net_revenue per driver,
-- also considering that new drivers with no revenue yet having a null value can be mistakened as an error.
-- So, i coverted it to 0
        coalesce(sum(case when status = 'completed' then net_revenue end),0) as total_net_revenue_earned,
        -- Averages: keep null — null means no completed trips to average over
        avg(case when status = 'completed' then net_revenue end) as avg_net_revenue_per_trip,
        avg(case when status = 'completed' then surge_multiplier end) as avg_surge_multiplier,

        -- this is to calculate fraud & anomaly indicators per driver
        count(case when extreme_surge_flag = 1 then 1 end) as extreme_surge_trips,
        count(case when corporate_trip_flag = 1 then 1 end)  as corporate_trips,

        -- this part is to detect various issues with payment after a completed trip such as failed payments, missing payment, and duplicate payment
        -- and it will be used to feed payment reliability report in the mart/final layer
        count(case when failed_payment_on_completed_trip_flag = 1 then 1 end) as failed_payment_count,
        count(case when missing_payment_flag = 1 then 1 end) as missing_payment_count,
        count(case when duplicate_payment_flag = 1 then 1 end) as duplicate_payment_count,
        -- Invalid payment amount: fee > amount (fraud/corruption signal)
        count(case when invalid_payment_amount_flag = 1 then 1 end) as invalid_payment_amount_count
    from {{ ref('trips_metrics') }}
    group by driver_id
),

-- this part will bw useful downstream to calculate the amount of online activities a driver has
online_sessions as (
    select driver_id, event_timestamp as online_at,
        lead(event_timestamp) over (partition by driver_id order by event_timestamp)  as offline_at, status
    from {{ ref('stg_driver_status_events') }}
),

online_hours as (
    -- Sum complete session pairs only (online → offline)
    -- Incomplete sessions (driver still online) are excluded via offline_at is not null
    select
        driver_id,
        round(sum({{ calculate_duration_minutes('online_at', 'offline_at') }} / 60.0),2
        ) as total_online_hours
    from online_sessions
    where status = 'online'
      and offline_at is not null
    group by driver_id
)

select
    -- for driver identity
    driver.driver_id, driver.city_id, driver.vehicle_id, driver.driver_status, driver.rating, driver.onboarding_date,

    -- here, it is reasonable to use coalesce to ensure that even new drivers with no trip at all equates to 0
    -- to ensure consistency in the data type
    coalesce(trip.driver_lifetime_trips, 0) as driver_lifetime_trips,
    coalesce(trip.cancelled_trips, 0) as cancelled_trips,
    coalesce(trip.no_show_trips, 0) as no_show_trips,
    coalesce(trip.total_trip_attempts, 0) as total_trip_attempts,

    -- drivers with no revenue value is coalesced to 0, ensuring that the data type is consistent instead of having a null
    coalesce(trip.total_net_revenue_earned, 0) as total_net_revenue_earned,

    --driver with no extreme trip value is replaced with 0 instead of null
    coalesce(trip.extreme_surge_trips, 0) as extreme_surge_trips,
    coalesce(trip.corporate_trips, 0) as corporate_trips,

    -- Payment reliability signals (coalesce to 0)
    coalesce(trip.failed_payment_count, 0) as failed_payment_count,
    coalesce(trip.missing_payment_count, 0) as missing_payment_count,
    coalesce(trip.duplicate_payment_count, 0) as duplicate_payment_count,
    coalesce(trip.invalid_payment_amount_count, 0) as invalid_payment_amount_count,

    -- Online availability (coalesce to 0 — never logged on = 0 hours)
    coalesce(online.total_online_hours, 0) as total_online_hours,

    -- Productivity (keep null — never been online ≠ 0 productivity)
    case
        when coalesce(online.total_online_hours, 0) = 0 then null
        else round(coalesce(trip.driver_lifetime_trips, 0)/ online.total_online_hours,2)
    end as trips_per_online_hour

from driver_base as driver
left join trip_metrics as trip
    on driver.driver_id = trip.driver_id
left join online_hours as online
    on driver.driver_id = online.driver_id