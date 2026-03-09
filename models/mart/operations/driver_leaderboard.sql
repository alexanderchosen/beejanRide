-- Grain: 1 row per driver
-- TAGS: operations
-- OWNER: operations team

{{ config(
    materialized='table',
    tags=['operations']
) }}

with driver_ranked as (

    select d.driver_id, d.city_id, d.vehicle_id, d.driver_status, d.driver_level, d.rating, d.onboarding_date, d.days_since_onboarding,

        -- Trip volume
        d.driver_lifetime_trips, d.cancelled_trips, d.no_show_trips, d.total_trip_attempts,

        -- Revenue
        d.total_net_revenue_earned,

        -- Availability
        d.total_online_hours, d.trips_per_online_hour,

        -- Fraud signals
        d.extreme_surge_trips, d.corporate_trips,

        -- Payment reliability
        d.failed_payment_count, d.missing_payment_count, d.duplicate_payment_count, d.invalid_payment_amount_count,

        -- Total payment issues — used for reliability ranking
        coalesce(d.failed_payment_count, 0)
        + coalesce(d.missing_payment_count, 0)
        + coalesce(d.duplicate_payment_count, 0)
        + coalesce(d.invalid_payment_amount_count, 0) as total_payment_issues,

        -- Rankings within city (only for active drivers with at least 1 trip)
        -- null for new drivers with no trips — rank not meaningful yet
        case
            when d.driver_lifetime_trips > 0 then
                rank() over (
                    partition by d.city_id
                    order by d.driver_lifetime_trips desc)
        end as city_trip_volume_rank,

        case
            when d.driver_lifetime_trips > 0 then
                rank() over (
                    partition by d.city_id
                    order by d.total_net_revenue_earned desc)
        end as city_revenue_rank,


        case
            when d.rating is not null then
                rank() over (
                    partition by d.city_id
                    order by d.rating desc)
        end  as city_rating_rank

    from {{ ref('dim_drivers') }} as d

)

select
    -- Identity
    r.driver_id, r.city_id, c.city_name, r.vehicle_id, r.driver_status, r.driver_level, r.rating, r.onboarding_date, r.days_since_onboarding,

    -- Trip volume
    r.driver_lifetime_trips, r.cancelled_trips, r.no_show_trips, r.total_trip_attempts,

    -- Revenue
    r.total_net_revenue_earned,

    -- Availability
    r.total_online_hours, r.trips_per_online_hour,

    -- Fraud signals
    r.extreme_surge_trips, r.corporate_trips,

    -- Payment reliability
    r.failed_payment_count, r.missing_payment_count, r.duplicate_payment_count, r.invalid_payment_amount_count, r.total_payment_issues,

    -- City rankings
    r.city_trip_volume_rank, r.city_revenue_rank, r.city_rating_rank

from driver_ranked as r
left join {{ ref('dim_cities') }} as c
    on r.city_id = c.city_id