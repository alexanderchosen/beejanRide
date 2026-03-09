-- Grain: 1 row per trip (with full payment event history joined)
-- TAGS: fraud
-- OWNER: fraud team

{{ config(
    materialized='table',
    tags=['fraud']
) }}

with payment_event_summary as (
    select
        trip_id, count(*) as total_payment_attempts,
        count(case when payment_status = 'success' then 1 end) as successful_payments,
        count(case when payment_status = 'failed' then 1 end)  as failed_payments,
        count(case when duplicate_payment_flag = 1 then 1 end) as duplicate_payment_events,
        count(case when invalid_amount_flag = 1 then 1 end) as invalid_amount_events,
        sum(amount)  as total_amount_attempted,
        sum(fee) as total_fee_attempted,
        max(payment_created_at) as last_payment_attempt_at
    from {{ ref('fact_payments') }}
    group by trip_id

)

select
    -- Trip identity
    t.trip_id, t.trip_date, t.city_id, t.driver_id, t.rider_id,t.status  as trip_status,

    -- Trip-grain payment outcome (from fct_trips — deduplicated)
    t.payment_status, t.payment_provider, t.payment_method, t.amount, t.fee, t.net_revenue,

    -- Trip-grain payment flags
    t.missing_payment_flag, t.duplicate_payment_flag, t.invalid_payment_amount_flag, t.failed_payment_on_completed_trip_flag,

    -- Raw payment event counts (from fct_payments — not deduplicated)
    coalesce(p.total_payment_attempts, 0) as total_payment_attempts,
    coalesce(p.successful_payments, 0) as successful_payments,
    coalesce(p.failed_payments, 0) as failed_payments,
    coalesce(p.duplicate_payment_events, 0) as duplicate_payment_events,
    coalesce(p.invalid_amount_events, 0) as invalid_amount_events,
    p.total_amount_attempted, p.total_fee_attempted, p.last_payment_attempt_at,

    -- Driver context
    d.driver_status, d.driver_level,
    d.city_id as driver_city_id,
    d.failed_payment_count as driver_lifetime_failed_payments,
    d.missing_payment_count  as driver_lifetime_missing_payments,

    -- Rider context
    r.rider_level, r.is_referred_rider,
    r.failed_payment_on_completed_count as rider_lifetime_failed_payments,
    r.missing_payment_count as rider_lifetime_missing_payments,

    -- Composite reliability flag: any payment issue present
    case
        when t.missing_payment_flag = 1
          or t.duplicate_payment_flag = 1
          or t.invalid_payment_amount_flag = 1
          or t.failed_payment_on_completed_trip_flag = 1 then 1
        else 0
    end as has_any_payment_issue

from {{ ref('fact_trips') }} as t
left join payment_event_summary as p
    on t.trip_id = p.trip_id
left join {{ ref('dim_drivers') }} as d
    on t.driver_id = d.driver_id
left join {{ ref('dim_riders') }} as r
    on t.rider_id = r.rider_id