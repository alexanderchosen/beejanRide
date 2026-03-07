with source_cte as (
    select *
    from {{ source('beejanride', 'driver_status_events_raw') }}

),

clean_cte as (
    select
        cast(event_id as int64) as event_id,
        cast(driver_id as int64) as driver_id,
        lower(trim(status)) as status,
        cast(event_timestamp as timestamp) as event_timestamp,
        cast(_airbyte_extracted_at as timestamp) as _airbyte_extracted_at
    from source_cte
    where event_id is not null

),

deduped_cte as (
    select *
    from clean_cte
    qualify row_number() over (
        partition by event_id
        order by event_timestamp desc
    ) = 1

)

select * from deduped_cte