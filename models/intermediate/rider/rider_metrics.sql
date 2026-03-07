-- Grain: 1 row per rider, including riders with zero trips
--
-- Sources:
--   stg_riders    → rider identity and attributes
--   trips_metrics → enriched trip data including net_revenue and all flags
--
-- NULL STRATEGY:
--   Counts & sums    → coalesce to 0  (a rider with no trips has 0 trips, £0 value)
--   Averages & rates → keep null      (null = no trips yet, 0 = trips with zero value)
--   referral_code    → keep null      (null = organically acquired rider, meaningful signal)
--   city_id          → most frequent trip city, null if rider has never taken a trip
--
-- DOWNSTREAM USAGE:
--   Rider LTV analysis       → rider_lifetime_value, avg_spend_per_trip, completion_rate_pct, is_referred_rider
--   Daily revenue dashboard  → rider_lifetime_value, country, corporate_trips
--   City-level profitability → most_frequent_city_id, rider_lifetime_value
--   Payment reliability      → missing_payment_count, failed_payment_on_completed_count, duplicate_payment_count
--   Fraud monitoring view    → extreme_surge_trips, missing_payment_count

{{ config(
    materialized='table'
) }}

-- so, i need specific information about the rider from the staging layer such as the rider's id, sign-up date, country, if the rider was referred or not, date the rider joined
with rider_base as (
    select rider_id, signup_date, country, referral_code, created_at
    from {{ ref('stg_riders') }}

),


trip_data as (

    -- Pull enriched trip data from trips_metrics intermediate model
    -- Using trips_metrics (not stg_trips) to leverage already-computed
    -- net_revenue, flags and business logic from that model

    select rider_id, trip_id, city_id, status, net_revenue, trip_duration_minutes, surge_multiplier,
        corporate_trip_flag, extreme_surge_flag, missing_payment_flag, duplicate_payment_flag,
        invalid_payment_amount_flag, failed_payment_on_completed_trip_flag
    from {{ ref('trips_metrics') }}

),

-- Derive each rider's most frequently visited city from their completed trips
-- Used for city-level profitability attribution in marts
-- Riders with zero trips will have no row here → coalesced to null in final select
city_preference as (
    select rider_id, city_id,
        row_number() over (partition by rider_id order by count(*) desc) as city_rank
    from trip_data
    where status = 'completed'
    group by rider_id, city_id

),

rider_metrics as (

    select r.rider_id, r.signup_date, r.country, r.referral_code, r.created_at,

        -- Most frequent city (null for riders with zero completed trips)
        c.city_id  as most_frequent_city_id,

        -- Trip volume counts (coalesce to 0)
        count(t.trip_id) as total_trips,
        count(case when t.status = 'completed' then 1 end) as completed_trips,
        count(case when t.status = 'cancelled' then 1 end) as cancelled_trips,
        count(case when t.status = 'no_show' then 1 end)  as no_show_trips,
        -- Lifetime value (coalesce to 0 — no trips = £0 value)
        coalesce(sum(case when t.status = 'completed' then t.net_revenue end),0) as rider_lifetime_value,
        -- Average metrics (keep null — null = no trips yet to average)
        avg(case when t.status = 'completed' then t.net_revenue end) as avg_spend_per_trip,
        avg(case when t.status = 'completed' then t.trip_duration_minutes end)as avg_trip_duration_minutes,
        avg(case when t.status = 'completed' then t.surge_multiplier end)  as avg_surge_multiplier,
        -- Corporate & surge behaviour (coalesce to 0)
        coalesce(sum(case when t.corporate_trip_flag = 1 then 1 end),0) as corporate_trips,
        coalesce(sum(case when t.extreme_surge_flag = 1 then 1 end),0) as extreme_surge_trips,
        -- Payment integrity signals (coalesce to 0)
        coalesce(sum(case when t.missing_payment_flag = 1 then 1 end),0) as missing_payment_count,
        coalesce(sum(case when t.duplicate_payment_flag = 1 then 1 end), 0) as duplicate_payment_count,
        coalesce(sum(case when t.invalid_payment_amount_flag = 1 then 1 end),0) as invalid_payment_amount_count,
        coalesce(sum(case when t.failed_payment_on_completed_trip_flag = 1 then 1 end),0) as failed_payment_on_completed_count,

        -- Completion rate:
        -- Denominator = completed + cancelled only (no_show excluded because
        -- no_show is the driver's fault in mobility platforms, not the rider's)
        -- Keep null for riders with zero trip attempts
        case when count(case when t.status in ('completed', 'cancelled') then 1 end
            ) = 0 then null
            else round(
                count(
                    case when t.status = 'completed' then 1 end
                ) * 100.0
                / count(
                    case when t.status in ('completed', 'cancelled') then 1 end
                ),
                2
            )
        end as completion_rate_pct,

        -- Referral flag: boolean derived from referral_code
        case
            when r.referral_code is not null then true
            else false
        end as is_referred_rider

    from rider_base as r
    left join trip_data as t
        on r.rider_id = t.rider_id
    left join city_preference as c
        on r.rider_id = c.rider_id
        and c.city_rank = 1
    group by
        r.rider_id, r.signup_date, r.country, r.referral_code, r.created_at, c.city_id

)
select * from rider_metrics