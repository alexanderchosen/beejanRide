-- Grain: 1 row per trip
-- Incremental on updated_at: captures both new trips and status changes on existing trips
--
-- MACROS USED:
--   calculate_duration_minutes(start_ts, end_ts) → timestamp_diff wrapper
--   calculate_net_revenue(amount_col, fee_col)   → (coalesce(amount,0) - coalesce(fee,0))
--   duplicate_payment_flag(trip_id)              → window count > 1 on raw payments
--
-- NULL STRATEGY:
--   trip_duration_minutes → 0 for non-completed trips (no pickup/dropoff occurred)
--   net_revenue           → 0 for trips with no payment record
--   amount, fee           → 0 for trips with no payment record
--   payment_status        → null for trips with no payment record (see missing_payment_flag)
--   all flag columns      → always 0 or 1, never null
--
-- DOWNSTREAM USAGE:
--   Daily revenue dashboard     → net_revenue, city_id, requested_at, status
--   City-level profitability    → city_id, net_revenue, corporate_trip_flag, surge_multiplier
--   Driver leaderboard          → driver_id, net_revenue, trip_duration_minutes, extreme_surge_flag
--   Rider LTV analysis          → rider_id, net_revenue, trip_duration_minutes, status
--   Payment reliability report  → payment_status, missing_payment_flag, duplicate_payment_flag,
--                                  invalid_payment_amount_flag
--   Fraud monitoring view       → extreme_surge_flag, duplicate_payment_flag,
--                                  missing_payment_flag, invalid_payment_amount_flag


with trips as (
    select *
    from {{ ref('stg_trips') }}

--    {% if is_incremental() %}
        -- Use updated_at to catch both new trips and status changes on existing trips
        -- Guarded against null updated_at using coalesce fallback to requested_at
        -- Note: is_incremental() guard prevents this filter running on first full load,
        -- which would cause (col > null = false) and load zero rows
  --      where coalesce(updated_at, requested_at) > (
--         select max(coalesce(updated_at, requested_at)) from {{ this }}
  --      )
   -- {% endif %}

),

payments_annotated as (

    -- Apply duplicate_payment_flag macro BEFORE deduplication
    -- The macro uses a window function: count(*) over (partition by trip_id) > 1
    -- After deduplication every trip has exactly 1 row, making the window count always 1
    -- So the flag MUST be computed here on the raw payment data

    select
        *,
        case
            when {{ duplicate_payment_flag('trip_id') }} then 1
            else 0
        end as is_duplicate_payment
    from {{ ref('stg_payments') }}

),

payments as (

    -- Deduplicate to 1 payment row per trip AFTER flagging duplicates above
    -- Priority: most recent successful payment first, then most recent attempt

    select *
    from (
        select
            *,
            row_number() over (
                partition by trip_id
                order by
                    case when payment_status = 'success' then 0 else 1 end,
                    created_at desc
            ) as recent_payment
        from payments_annotated
    )
    where recent_payment = 1

),

tp_joined as (

    select
        -- Keys
        t.trip_id,
        t.driver_id,
        t.rider_id,
        t.city_id,

        -- Trip attributes
        t.status,
        t.requested_at,
        t.pickup_at,
        t.dropoff_at,
        t.surge_multiplier,
        t.is_corporate,
        t.payment_method,

        -- Timestamps needed for incremental cursor (must be in final select)
        t.updated_at,
        t.created_at,

        -- Payment attributes
        -- payment_status and payment_provider are intentionally nullable:
        -- null means no payment record exists (see missing_payment_flag below)
        p.payment_id,
        p.payment_status,
        p.payment_provider,
        p.currency,
        coalesce(p.amount, 0) as amount,
        coalesce(p.fee, 0) as fee,

        -- Duration: macro call wrapped in coalesce → 0 for cancelled/no_show trips
        -- A completed trip with 0 duration will be caught by trip_duration_positive test
        coalesce(
            {{ calculate_duration_minutes('t.pickup_at', 't.dropoff_at') }},0) as trip_duration_minutes,

        -- Net revenue: macro-computed (amount - fee)
        -- NOT floored at 0 — fee > amount is flagged explicitly below as fraud
        -- The no_negative_revenue custom test will catch any negative values post-build
        {{ calculate_net_revenue('p.amount', 'p.fee') }} as net_revenue,

        -- Corporate flag
        case when t.is_corporate = true then 1
            else 0 end as corporate_trip_flag,

        -- Missing payment flag: trip exists but no payment record found at all
        -- Distinct from duplicate_payment (opposite fraud signals)
        case when p.trip_id is null then 1
            else 0 end as missing_payment_flag,

        -- Duplicate payment flag: carried through from payments_annotated CTE
        -- Computed on raw stg_payments before deduplication (see payments_annotated CTE)
        coalesce(p.is_duplicate_payment, 0) as duplicate_payment_flag,

        -- Invalid payment amount flag: fee exceeds charged amount
        -- Mathematically impossible in legitimate Stripe/PayPal transactions
        -- Indicates data corruption, fee manipulation or chargeback misrecording
        -- Flagged as fraud signal rather than silently corrected
        case
            when p.trip_id is not null and coalesce(p.fee, 0) > coalesce(p.amount, 0) then 1
            else 0 end as invalid_payment_amount_flag,

        -- Failed payment on completed trip:
        -- Null-safe: coalesce treats missing payment_status as non-success
        -- Catches both explicit failures AND trips with no payment record at all
        case
            when t.status = 'completed' and coalesce(p.payment_status, 'missing') != 'success' then 1
            else 0 end as failed_payment_on_completed_trip_flag,

        -- Extreme surge flag (anomaly/fraud indicator per requirements)
        case
            when t.surge_multiplier > 10 then 1
            else 0 end as extreme_surge_flag

    from trips as t
    left join payments as p
        on t.trip_id = p.trip_id

)

select * from tp_joined