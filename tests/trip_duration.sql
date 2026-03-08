-- Custom test: trip_duration_positive
--
-- PASSES when: zero rows returned
-- FAILS when:  one or more rows returned
--
-- Business rule:
--   Every completed trip must have a positive trip duration.
--   A completed trip with duration = 0 or negative is physically impossible
--   and indicates a data pipeline issue — likely null or inverted
--   pickup_at / dropoff_at timestamps in the source system.
--
--   Non-completed trips (cancelled, no_show) are explicitly excluded —
--   they legitimately have trip_duration_minutes = 0 since no pickup
--   or dropoff ever occurred.
--
-- Runs on: trips_metrics (intermediate layer)
--   Catches the problem early before it propagates to fct_trips and mart models.
--
-- Referenced in: int_schema.yml under trips_metrics model-level data_tests

select *
from {{ ref('trips_metrics') }}
where status = 'completed'
  and trip_duration_minutes <= 0