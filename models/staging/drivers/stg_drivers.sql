with source_cte as (
    select *
    from {{ source('beejanride', 'drivers_raw') }}

),

clean_cte as (
    select
        -- Primary Key
        cast(driver_id as int64) as driver_id,

        -- Foreign Keys
        cast(city_id as int64) as city_id,
        cast(vehicle_id as string) as vehicle_id,

        -- Business Attributes
        cast(onboarding_date as date) as onboarding_date,
        lower(trim(driver_status)) as driver_status,
        cast(rating as float64) as rating,

        -- Timestamps
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,

        -- Airbyte metadata (for fallback dedup)
        _airbyte_extracted_at
    from source_cte
    where driver_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by driver_id
        order by updated_at desc, _airbyte_extracted_at desc
    ) = 1

)

select
    driver_id,
    city_id,
    vehicle_id,
    onboarding_date,
    driver_status,
    rating,
    created_at,
    updated_at
from deduped_cte