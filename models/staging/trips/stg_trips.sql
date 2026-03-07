with source_cte as (
    select *
    from {{ source('beejanride', 'trips_raw') }}

),

clean_cte as (
    select
        cast(trip_id as int64) as trip_id,
        cast(rider_id as int64) as rider_id,
        cast(driver_id as int64) as driver_id,
        cast(vehicle_id as string) as vehicle_id,
        cast(city_id as int64) as city_id,
        cast(requested_at as timestamp) as requested_at,
        cast(pickup_at as timestamp) as pickup_at,
        cast(dropoff_at as timestamp) as dropoff_at,
        lower(trim(status)) as status,
        cast(estimated_fare as numeric) as estimated_fare,
        cast(actual_fare as numeric) as actual_fare,
        cast(surge_multiplier as numeric) as surge_multiplier,
        lower(trim(payment_method)) as payment_method,
        cast(is_corporate as boolean) as is_corporate,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(_airbyte_extracted_at as timestamp) as _airbyte_extracted_at
    from source_cte
    where trip_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by trip_id
        order by updated_at desc
    ) = 1

)

select * from deduped_cte