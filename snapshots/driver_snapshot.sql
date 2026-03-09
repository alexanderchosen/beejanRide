{% snapshot drivers_snapshot %}

{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='driver_id',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select *
from {{ ref('stg_drivers') }}

{% endsnapshot %}