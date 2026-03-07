with source_cte as (
    select *
    from {{ source('beejanride', 'riders_raw') }}

),

clean_cte as (
    select
        cast(rider_id as int64) as rider_id,
        cast(signup_date as date) as signup_date,
        upper(trim(country)) as country,
        lower(trim(referral_code)) as referral_code,
        cast(created_at as timestamp) as created_at,
        cast(_airbyte_extracted_at as timestamp) as _airbyte_extracted_at
    from source_cte
    where rider_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by rider_id
        order by created_at desc
    ) = 1

)

select * from deduped_cte