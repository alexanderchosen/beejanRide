-- Grain: 1 row per city

{{ config(
    materialized='table',
    tags=['finance']
) }}

with city_trip_metrics as (

    select
        f.city_id,

        -- Volume
        count(*) as total_trips,
        count(case when f.status = 'completed' then 1 end) as completed_trips,
        count(case when f.status = 'cancelled' then 1 end) as cancelled_trips,

        -- Revenue
        sum(case when f.status = 'completed'
            then f.net_revenue else 0 end) as total_net_revenue,

        -- Corporate split
        sum(case when f.corporate_trip_flag = 1
            then f.net_revenue else 0 end) as corporate_net_revenue,
        count(case when f.corporate_trip_flag = 1 then 1 end) as corporate_trips,

        -- Surge metrics
        count(case when f.extreme_surge_flag = 1 then 1 end) as extreme_surge_trips,

        -- Payment reliability
        count(case when f.missing_payment_flag = 1 then 1 end) as missing_payment_trips,
        count(case when f.failed_payment_on_completed_trip_flag = 1
            then 1 end) as failed_payment_trips,

        -- Date range
        min(f.trip_date) as first_trip_date,
        max(f.trip_date) as last_trip_date

    from {{ ref('fact_trips') }} as f
    group by f.city_id

),

city_driver_metrics as (

    -- Driver supply metrics per city
    select
        city_id,
        count(*) as total_drivers,
        count(case when driver_status = 'active' then 1 end) as active_drivers,
        sum(total_online_hours) as total_driver_online_hours,
        sum(driver_lifetime_trips) as total_driver_lifetime_trips
    from {{ ref('dim_drivers') }}
    group by city_id

)

select
    -- City identity
    c.city_id, c.city_name, c.country, c.launch_date, c.days_since_launch,

    -- Driver supply
    coalesce(dr.total_drivers, 0) as total_drivers,
    coalesce(dr.active_drivers, 0) as active_drivers,

    -- Trip volume
    coalesce(t.total_trips, 0) as total_trips,
    coalesce(t.completed_trips, 0) as completed_trips,
    coalesce(t.cancelled_trips, 0) as cancelled_trips,

    -- Revenue
    coalesce(t.total_net_revenue, 0) as total_net_revenue,

    -- Maturity-normalised revenue — revenue per day since launch
    case
        when coalesce(c.days_since_launch, 0) = 0 then null
        else round(coalesce(t.total_net_revenue, 0) / c.days_since_launch,2)
    end as revenue_per_day_since_launch,

    -- Corporate split
    coalesce(t.corporate_net_revenue, 0) as corporate_net_revenue,
    coalesce(t.corporate_trips, 0) as corporate_trips,

    -- Surge
    coalesce(t.extreme_surge_trips, 0) as extreme_surge_trips,

    -- Driver efficiency
    case
        when coalesce(dr.total_driver_online_hours, 0) = 0 then null
        else round(coalesce(t.completed_trips, 0) / dr.total_driver_online_hours,2)
    end as trips_per_driver_online_hour,

    -- Payment reliability
    coalesce(t.missing_payment_trips, 0) as missing_payment_trips,
    coalesce(t.failed_payment_trips, 0) as failed_payment_trips,

    -- Activity window
    t.first_trip_date, t.last_trip_date

from {{ ref('dim_cities') }} as c
left join city_trip_metrics as t
    on c.city_id = t.city_id
left join city_driver_metrics as dr
    on c.city_id = dr.city_id