

with source as (
    select *
    from {{ ref('trips_metrics') }}

    
)

select
    -- Primary key
    trip_id,
    -- Foreign keys (dimensional references — joins happen at mart/BI layer)
    driver_id, rider_id, city_id,

    -- Trip date keys — pre-extracted for dashboard grouping performance
    date(requested_at) as trip_date,
    extract(year from requested_at)  as trip_year,
    extract(month from requested_at) as trip_month,
    extract(dayofweek from requested_at) as trip_day_of_week,

    -- Trip attributes
    status, payment_status, payment_provider, payment_method, surge_multiplier,

    -- Measures
    trip_duration_minutes, amount, fee, net_revenue,

    -- Fraud & integrity flags (0/1)
    corporate_trip_flag, missing_payment_flag, duplicate_payment_flag, invalid_payment_amount_flag, failed_payment_on_completed_trip_flag, extreme_surge_flag,

    -- Timestamps
    requested_at, updated_at
from source