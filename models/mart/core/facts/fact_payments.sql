with payments as (
    select p.payment_id, p.trip_id, p.payment_status, p.payment_provider, p.amount, p.fee, p.currency, p.created_at as payment_created_at,
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

  --  {% if is_incremental() %}
   --     where p.created_at > (
   --         select max(payment_created_at)
   --         from {{ this }}
  --      )
   -- {% endif %}

),
-- Join to fact_trips to inherit dimensional context
-- Avoids reaching back to stg_trips for trip_date and entity FKs
trip_context as (
    select
        trip_id, trip_date, city_id, driver_id, rider_id
    from {{ ref('fact_trips') }}

)

select
    -- Primary key
    p.payment_id,
    -- Foreign keys
    p.trip_id, t.driver_id, t.rider_id, t.city_id,

    -- Date key (inherited from fct_trips)
    t.trip_date as payment_date,
    -- Payment attributes
    p.payment_status, p.payment_provider, p.currency,

    -- Measures (amount and fee only — no net_revenue at payment grain)
    p.amount, p.fee,

    -- Integrity flags
    p.invalid_amount_flag, p.duplicate_payment_flag,

    -- Timestamps
    p.payment_created_at

from payments as p
left join trip_context as t
    on p.trip_id = t.trip_id