-- Custom test: completed_trip_must_have_successful_payment
--
-- PASSES when: zero rows returned
-- FAILS when:  one or more rows returned
--
-- Business rule:
--   Every completed trip must have a corresponding successful payment record.
--   A completed trip with a failed or missing payment indicates either:
--     (1) A payment processing failure that was not caught before trip completion
--     (2) A data pipeline issue where the payment record has not yet landed
--     (3) A potential fraud scenario — service delivered without payment collected
--
-- Null safety:
--   coalesce(payment_status, 'missing') treats null payment_status the same
--   as a failed payment — both are unacceptable on a completed trip.
--   This catches trips where the LEFT JOIN to stg_payments returned no match
--   (missing_payment_flag = 1) in addition to explicit payment failures.
--
-- Runs on: trips_metrics (intermediate layer)
--   Catches the problem early before it propagates to fct_trips and mart models.
--
-- Referenced in: int_schema.yml under trips_metrics model-level data_tests

select *
from {{ ref('trips_metrics') }}
where status = 'completed'
  and coalesce(payment_status, 'missing') != 'success'