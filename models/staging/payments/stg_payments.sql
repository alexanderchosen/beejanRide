with source_cte as (
    select *
    from {{ source('beejanride', 'payments_raw') }}

),

clean_cte as (
    select
        cast(payment_id as int64) as payment_id,
        cast(trip_id as int64) as trip_id,
        lower(trim(payment_status)) as payment_status,
        lower(trim(payment_provider)) as payment_provider,
        cast(amount as numeric) as amount,
        cast(fee as numeric) as fee,
        upper(trim(currency)) as currency,
        cast(created_at as timestamp) as created_at,
        cast(_airbyte_extracted_at as timestamp) as _airbyte_extracted_at
    from source_cte
    where payment_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by payment_id
        order by created_at desc
    ) = 1

)

select * from deduped_cte