-- Grain: 1 row per flagged trip
--
-- Sources:
--   fct_trips   → trip-grain fraud flags
--   dim_drivers → driver identity and reliability signals
--   dim_riders  → rider identity and reliability signals
--   dim_cities  → city context
--
-- PURPOSE:
--   Live fraud monitoring feed for the fraud team.
--   Filters fct_trips to only trips with at least one active fraud flag,
--   enriches with driver and rider context to support rapid triage.
--   Operations team uses this to prioritise investigation queues daily.
--
--   Fraud signals tracked:
--     extreme_surge_flag                    → surge multiplier > 10
--     duplicate_payment_flag                → > 1 payment record for trip
--     invalid_payment_amount_flag           → fee > amount (impossible legitimately)
--     missing_payment_flag                  → completed trip with no payment record
--     failed_payment_on_completed_trip_flag → completed trip with failed payment
--
-- Materialisation: incremental (merge on trip_id)
--   New flagged trips append daily — fraud team always sees latest flags.
--   Merge on trip_id handles cases where a trip is retrospectively flagged
--   (e.g. a duplicate payment arrives the day after the trip).
--
-- TAGS: fraud
-- OWNER: fraud team

{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    tags=['fraud']
) }}

with flagged_trips as (

    select *
    from {{ ref('fact_trips') }}

    where
        extreme_surge_flag = 1
        or duplicate_payment_flag = 1
        or invalid_payment_amount_flag = 1
        or missing_payment_flag = 1
        or failed_payment_on_completed_trip_flag = 1

    {% if is_incremental() %}
        and coalesce(updated_at, requested_at) > (
            select max(coalesce(updated_at, requested_at))
            from {{ this }}
        )
    {% endif %}

)

select
    -- Trip identity
    f.trip_id,
    f.trip_date,
    f.city_id,
    c.city_name,
    f.driver_id,
    f.rider_id,
    f.status as trip_status,

    -- Active fraud flags (all 0/1)
    f.extreme_surge_flag,
    f.duplicate_payment_flag,
    f.invalid_payment_amount_flag,
    f.missing_payment_flag,
    f.failed_payment_on_completed_trip_flag,

    -- Flag count — triage priority signal (more flags = higher priority)
    f.extreme_surge_flag
    + f.duplicate_payment_flag
    + f.invalid_payment_amount_flag
    + f.missing_payment_flag
    + f.failed_payment_on_completed_trip_flag as total_flags,

    -- Trip financials
    f.surge_multiplier,
    f.amount,
    f.fee,
    f.net_revenue,
    f.payment_status,
    f.payment_provider,
    f.payment_method,

    -- Driver context for triage
    d.driver_status,
    d.driver_tier,
    d.rating                                                    as driver_rating,
    d.extreme_surge_trips                                       as driver_lifetime_extreme_surges,
    d.failed_payment_count                                      as driver_lifetime_failed_payments,
    d.invalid_payment_amount_count                              as driver_lifetime_invalid_payments,

    -- Rider context for triage
    r.rider_tier,
    r.is_referred_rider,
    r.failed_payment_on_completed_count                         as rider_lifetime_failed_payments,
    r.missing_payment_count                                     as rider_lifetime_missing_payments,

    -- Timestamps
    f.requested_at,
    f.updated_at

from flagged_trips as f
left join {{ ref('dim_cities') }} as c
    on f.city_id = c.city_id
left join {{ ref('dim_drivers') }} as d
    on f.driver_id = d.driver_id
left join {{ ref('dim_riders') }} as r
    on f.rider_id = r.rider_id