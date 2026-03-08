-- Custom test: no_negative_revenue
--
-- PASSES when: zero rows returned
-- FAILS when:  one or more rows returned
--
-- Business rule:
--   net_revenue should never be negative unless the trip has been explicitly
--   flagged as an invalid payment amount (fee > amount).
--   Negative net_revenue on invalid_payment_amount_flag = 1 trips is
--   intentional — it is preserved as a fraud signal for investigation.
--   Negative net_revenue on any other trip indicates unexpected data corruption.
--
-- Runs on: trips_metrics (intermediate layer)
--   Catches the problem early before it propagates to fct_trips and mart models.
--
-- Referenced in: int_schema.yml under trips_metrics model-level data_tests

select *
from {{ ref('trips_metrics') }}
where net_revenue < 0
  and invalid_payment_amount_flag = 0