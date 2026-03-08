-- Grain: 1 row per payment event (NOT deduplicated)
--
-- Sources:
--   stg_payments  → raw payment events including duplicates and failures
--   fct_trips     → provides trip_date, city_id, driver_id, rider_id
--                   for dimensional context without re-joining to stg_trips
--
-- IMPORTANT — why NOT deduplicated:
--   trips_metrics already handles payment deduplication at trip grain to
--   derive a single clean net_revenue per trip. fct_payments intentionally
--   preserves the raw payment grain so mart_payment_reliability can see
--   every payment attempt — duplicates, failures, and invalid amounts are
--   all meaningful events for investigation.
--
-- IMPORTANT — why no net_revenue here:
--   net_revenue is a trip-grain measure and lives exclusively on fct_trips.
--   A failed payment or duplicate payment has no revenue — computing
--   net_revenue at payment grain would be misleading and imply revenue
--   on events that represent no actual income. Revenue reporting always
--   goes through fct_trips, never fct_payments.
--
-- Materialisation: incremental (merge on payment_id)
--   Payments are immutable once written — a payment record is never
--   updated after creation, only new payments are added.
--   Merge with payment_id unique key guards against Airbyte duplicate
--   ingestion even though append would theoretically be safe.
--
-- DOWNSTREAM USAGE:
--   mart_payment_reliability → full payment event history per trip/driver/rider
--   mart_fraud_monitoring    → duplicate and invalid payment signals

{{ config(
    materialized='incremental',
    unique_key='payment_id',
    incremental_strategy='merge'
) }}

with payments as (

    select
        p.payment_id,
        p.trip_id,
        p.payment_status,
        p.payment_provider,
        p.amount,
        p.fee,
        p.currency,
        p.created_at as payment_created_at,

        -- Invalid amount flag: fee exceeds charged amount
        -- Mathematically impossible in legitimate transactions
        -- Fraud/data corruption signal
        case
            when coalesce(p.fee, 0) > coalesce(p.amount, 0) then 1
            else 0
        end as invalid_amount_flag,

        -- Duplicate signal: more than one payment record exists for this trip
        -- Computed on raw stg_payments before any deduplication
        case
            when count(*) over (partition by p.trip_id) > 1 then 1
            else 0
        end as duplicate_payment_flag

    from {{ ref('stg_payments') }} as p

    {% if is_incremental() %}
        where p.created_at > (
            select max(payment_created_at)
            from {{ this }}
        )
    {% endif %}

),

-- Join to fact_trips to inherit dimensional context
-- Avoids reaching back to stg_trips for trip_date and entity FKs
trip_context as (
    select
        trip_id,
        trip_date,
        city_id,
        driver_id,
        rider_id
    from {{ ref('fact_trips') }}

)

select
    -- Primary key
    p.payment_id,

    -- Foreign keys
    p.trip_id,
    t.driver_id,
    t.rider_id,
    t.city_id,

    -- Date key (inherited from fct_trips)
    t.trip_date as payment_date,

    -- Payment attributes
    p.payment_status,
    p.payment_provider,
    p.currency,

    -- Measures (amount and fee only — no net_revenue at payment grain)
    p.amount,
    p.fee,

    -- Integrity flags
    p.invalid_amount_flag,
    p.duplicate_payment_flag,

    -- Timestamps
    p.payment_created_at

from payments as p
left join trip_context as t
    on p.trip_id = t.trip_id