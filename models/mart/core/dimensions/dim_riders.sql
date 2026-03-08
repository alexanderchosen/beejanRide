-- Grain: 1 row per rider (current state)
--
-- Sources:
--   stg_riders     → identity, signup date, country, referral code
--   rider_metrics  → aggregated behaviour metrics for BI reporting
--
-- This is a Type 1 SCD — always reflects current rider state.
-- Riders have no snapshot requirement (unlike drivers) since
-- rider attributes (country, referral status) are immutable after signup.
--
-- Materialisation: table
--   Full rider population always needed for FK integrity.
--   Dimension tables must never be incremental — partial loads
--   would break joins in fct_trips for riders with no recent trips.
--
-- DOWNSTREAM USAGE:
--   fct_trips        → FK join on rider_id
--   mart_rider_ltv   → lifetime value, referral cohort analysis
--   mart_fraud_monitoring → payment integrity signals at rider grain

{{ config(
    materialized='table'
) }}

select
    -- Identity (from stg_riders)
    r.rider_id, r.country, r.signup_date, r.referral_code,
    -- Derived: days since signup — rider tenure signal
    date_diff(current_date(), r.signup_date, day)       as days_since_signup,

    -- Derived: referral flag from referral_code presence
    -- Mirrors is_referred_rider from rider_metrics for direct dim access
    r.referral_code is not null as is_referred_rider,

    -- Derived: rider tier based on lifetime completed trips
    -- Used in mart_rider_ltv for cohort segmentation
    case
        when coalesce(m.completed_trips, 0) = 0 then 'new'
        when coalesce(m.completed_trips, 0) <=1 then 'occasional'
        when coalesce(m.completed_trips, 0) <=3 then 'regular'
        else 'frequent'
    end as rider_level,

    -- Trip volume (from rider_metrics)
    coalesce(m.total_trips, 0) as total_trips,
    coalesce(m.completed_trips, 0) as completed_trips,
    coalesce(m.cancelled_trips, 0) as cancelled_trips,
    coalesce(m.no_show_trips, 0)as no_show_trips,

    -- Revenue (from rider_metrics)
    coalesce(m.rider_lifetime_value, 0) as rider_lifetime_value,
    m.avg_spend_per_trip,                               -- keep null: no trips yet

    -- Preferred city (from rider_metrics)
    m.most_frequent_city_id,                            -- keep null: no completed trips

    -- Performance rates (from rider_metrics)
    m.completion_rate_pct,                              -- keep null: no attempts yet
    m.cancellation_rate_pct,                            -- keep null: no attempts yet

    -- Surge & corporate (from rider_metrics)
    coalesce(m.extreme_surge_trips, 0) as extreme_surge_trips,
    coalesce(m.corporate_trips, 0) as corporate_trips,

    -- Payment reliability (from rider_metrics)
    coalesce(m.missing_payment_count, 0) as missing_payment_count,
    coalesce(m.duplicate_payment_count, 0) as duplicate_payment_count,
    coalesce(m.failed_payment_on_completed_count, 0) as failed_payment_on_completed_count,
    coalesce(m.invalid_payment_amount_count, 0) as invalid_payment_amount_count

from {{ ref('stg_riders') }} as r
left join {{ ref('rider_metrics') }} as m
    on r.rider_id = m.rider_id