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

-- SCD Type 2 snapshot for drivers
--
-- Tracks historical changes to driver attributes over time:
--   driver_status → active / suspended / inactive
--   vehicle_id    → vehicle reassignments (e.g. VH001 → VH002)
--   rating        → rating updates over time
--
-- STRATEGY: timestamp
--   dbt uses the updated_at column exclusively to detect changes.
--   When updated_at changes on a driver row, dbt:
--     1. Closes the existing record by setting dbt_valid_to = current_timestamp
--     2. Inserts a new record with dbt_valid_from = current_timestamp
--        and dbt_valid_to = null (the new current active record)
--
-- WHY select *:
--   The timestamp strategy does not compare column values — it only
--   checks updated_at. Column selection does not control what triggers
--   a new snapshot row. Selecting * ensures the complete driver record
--   is captured at every point in time, so point-in-time joins downstream
--   have access to all attributes without exception.
--
-- dbt_valid_to = null always means the current active record for that driver.
--
-- SOURCE: stg_drivers (not dim_drivers)
--   Snapshots should always source from the cleanest available grain —
--   staging in this case. Sourcing from dim_drivers would create a
--   dependency on a marts-layer model, violating dbt's layering principle.
--
-- invalidate_hard_deletes: True
--   If a driver is deleted from the source, dbt closes their snapshot
--   record rather than leaving it open with dbt_valid_to = null indefinitely.
--
-- DOWNSTREAM USAGE:
--   Point-in-time analysis  → join on driver_id WHERE dbt_valid_from <= event_time
--                             AND (dbt_valid_to > event_time OR dbt_valid_to IS NULL)
--   Suspension duration     → dbt_valid_to - dbt_valid_from WHERE driver_status = 'suspended'
--   Vehicle change tracking → filter on vehicle_id changes across snapshot rows
--   Rating trend analysis   → rating values ordered by dbt_valid_from per driver

select *
from {{ ref('stg_drivers') }}

{% endsnapshot %}