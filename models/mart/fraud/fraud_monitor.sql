-- Grain: 1 row per flagged trip
-- TAGS: fraud
-- OWNER: fraud team


with flagged_trips as (

    select *
    from {{ ref('fact_trips') }}

    where
        extreme_surge_flag = 1
        or duplicate_payment_flag = 1
        or invalid_payment_amount_flag = 1
        or missing_payment_flag = 1
        or failed_payment_on_completed_trip_flag = 1

--    {% if is_incremental() %}
--        and coalesce(updated_at, requested_at) > (
--            select max(coalesce(updated_at, requested_at))
--            from {{ this }}
--        )
--    {% endif %}

)

select
    -- Trip identity
    f.trip_id, f.trip_date, f.city_id, c.city_name, f.driver_id, f.rider_id, f.status as trip_status,

    -- Active fraud flags (all 0/1)
    f.extreme_surge_flag, f.duplicate_payment_flag, f.invalid_payment_amount_flag, f.missing_payment_flag, f.failed_payment_on_completed_trip_flag,

    -- Flag count — triage priority signal (more flags = higher priority)
    f.extreme_surge_flag
    + f.duplicate_payment_flag
    + f.invalid_payment_amount_flag
    + f.missing_payment_flag
    + f.failed_payment_on_completed_trip_flag as total_flags,

    -- Trip financials
    f.surge_multiplier, f.amount, f.fee, f.net_revenue, f.payment_status, f.payment_provider, f.payment_method,

    -- Driver context for triage
    d.driver_status, d.driver_level, d.rating as driver_rating,
    d.extreme_surge_trips as driver_lifetime_extreme_surges,
    d.failed_payment_count as driver_lifetime_failed_payments,
    d.invalid_payment_amount_count as driver_lifetime_invalid_payments,

    -- Rider context for triage
    r.rider_level, r.is_referred_rider,
    r.failed_payment_on_completed_count as rider_lifetime_failed_payments,
    r.missing_payment_count as rider_lifetime_missing_payments,

    -- Timestamps
    f.requested_at, f.updated_at

from flagged_trips as f
left join {{ ref('dim_cities') }} as c
    on f.city_id = c.city_id
left join {{ ref('dim_drivers') }} as d
    on f.driver_id = d.driver_id
left join {{ ref('dim_riders') }} as r
    on f.rider_id = r.rider_id