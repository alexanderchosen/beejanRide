with source_cte as (
    select *
    from {{ source('beejanride', 'cities_raw') }}

),

clean_cte as (
    select
        cast(city_id as int64) as city_id,
        lower(trim(city_name)) as city_name,
        upper(trim(country)) as country,
        cast(launch_date as date) as launch_date,
        _airbyte_extracted_at
    from source_cte
    where city_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by city_id
        order by _airbyte_extracted_at desc
    ) = 1

)

select
    city_id,
    city_name,
    country,
    launch_date
from deduped_cte