-- Grain: 1 row per trip


-- I implemented the macros here


with trips as (
    select *
    from {{ ref('stg_trips') }}

      -- {% if is_incremental() %}
    --    where coalesce(updated_at, requested_at) > (
    --     select max(coalesce(updated_at, requested_at)) from {{ this }}
    --    )
   --{% endif %}

),

payments_annotated as (
    select
        *,
        case
            when {{ duplicate_payment_flag('trip_id') }} then 1
            else 0 end as is_duplicate_payment
    from {{ ref('stg_payments') }}
),

payments as (
    select *
    from (
        select *,
            row_number() over (
                partition by trip_id order by
                case when payment_status = 'success' then 0 else 1 end,
                created_at desc
            ) as recent_payment
        from payments_annotated)
        where recent_payment = 1
),

tp_joined as (
    select
        -- Keys
        t.trip_id, t.driver_id, t.rider_id, t.city_id,

        -- Trip attributes
        t.status, t.requested_at, t.pickup_at, t.dropoff_at, t.surge_multiplier, t.is_corporate, t.payment_method,
        t.updated_at, t.created_at,

        -- Payment attributes
        p.payment_id, p.payment_status, p.payment_provider, p.currency,
        coalesce(p.amount, 0) as amount,
        coalesce(p.fee, 0) as fee,

        -- A completed trip with 0 duration will be caught by trip_duration macro
        coalesce({{ calculate_duration_minutes('t.pickup_at', 't.dropoff_at') }},0) as trip_duration_minutes,

        -- Net revenue: macro-computed (amount - fee)
        {{ calculate_net_revenue('p.amount', 'p.fee') }} as net_revenue,

        -- Corporate flag
        case when t.is_corporate = true then 1
            else 0 end as corporate_trip_flag,

        -- Missing payment flag
        case when p.trip_id is null then 1
            else 0 end as missing_payment_flag,

        -- Duplicate payment flag
        coalesce(p.is_duplicate_payment, 0) as duplicate_payment_flag,

        -- Invalid payment amount flag
        case
            when p.trip_id is not null and coalesce(p.fee, 0) > coalesce(p.amount, 0) then 1
            else 0 end as invalid_payment_amount_flag,

        -- Failed payment on completed trip
        case when t.status = 'completed' and coalesce(p.payment_status, 'missing') != 'success' then 1
            else 0 end as failed_payment_on_completed_trip_flag,

        -- Extreme surge flag (anomaly/fraud indicator per requirements)
        case when t.surge_multiplier > 10 then 1
            else 0 end as extreme_surge_flag

    from trips as t
    left join payments as p
        on t.trip_id = p.trip_id

)

select * from tp_joined