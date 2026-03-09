{{ config(
    materialized='table'
) }}

select
    -- Rider Identity (from stg_riders)
    r.rider_id, r.country, r.signup_date, r.referral_code,
    -- days since signup — rider tenure signal
    date_diff(current_date(), r.signup_date, day) as days_since_signup,

    -- referral flag from referral_code presence
    r.referral_code is not null as is_referred_rider,

   -- rider tier based on lifetime completed trips
    case
        when coalesce(m.completed_trips, 0) = 0 then 'new'
        when coalesce(m.completed_trips, 0) <=1 then 'occasional'
        when coalesce(m.completed_trips, 0) <=3 then 'regular'
        else 'frequent'
    end as rider_level,

    -- Trip volume from rider_metrics
    coalesce(m.total_trips, 0) as total_trips,
    coalesce(m.completed_trips, 0) as completed_trips,
    coalesce(m.cancelled_trips, 0) as cancelled_trips,
    coalesce(m.no_show_trips, 0)as no_show_trips,

    -- Revenue from rider_metrics
    coalesce(m.rider_lifetime_value, 0) as rider_lifetime_value,

    -- Preferred city from rider_metrics
    m.most_frequent_city_id,

    -- Surge & corporate from rider_metrics
    coalesce(m.extreme_surge_trips, 0) as extreme_surge_trips,
    coalesce(m.corporate_trips, 0) as corporate_trips,

    -- Payment reliability from rider_metrics
    coalesce(m.missing_payment_count, 0) as missing_payment_count,
    coalesce(m.duplicate_payment_count, 0) as duplicate_payment_count,
    coalesce(m.failed_payment_on_completed_count, 0) as failed_payment_on_completed_count,
    coalesce(m.invalid_payment_amount_count, 0) as invalid_payment_amount_count

from {{ ref('stg_riders') }} as r
left join {{ ref('rider_metrics') }} as m
    on r.rider_id = m.rider_id