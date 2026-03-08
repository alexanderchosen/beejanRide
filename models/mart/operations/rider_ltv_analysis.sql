-- Grain: 1 row per rider
--
-- Sources:
--   dim_riders  → all rider metrics already aggregated
--   dim_cities  → city_name for most frequent city display
--
-- PURPOSE:
--   Rider lifetime value analysis for the operations team.
--   Segments riders by value tier and referral cohort to support
--   retention decisions, referral programme evaluation, and
--   identification of high-value riders at risk of churn.
--
-- Materialisation: table
--   Full rider population needed for accurate LTV distribution.
--   Percentile calculations require the complete dataset.
--
-- TAGS: operations
-- OWNER: operations team

{{ config(
    materialized='table',
    tags=['operations']
) }}

with rider_valued as (

    select
        r.rider_id,
        r.country,
        r.signup_date,
        r.days_since_signup,
        r.referral_code,
        r.is_referred_rider,
        r.rider_tier,

        -- Trip behaviour
        r.total_trips,
        r.completed_trips,
        r.cancelled_trips,
        r.no_show_trips,
        r.completion_rate_pct,
        r.cancellation_rate_pct,

        -- Revenue
        r.rider_lifetime_value,
        r.avg_spend_per_trip,

        -- Preferred city
        r.most_frequent_city_id,

        -- Surge & corporate
        r.extreme_surge_trips,
        r.corporate_trips,

        -- Payment integrity
        r.missing_payment_count,
        r.duplicate_payment_count,
        r.failed_payment_on_completed_count,
        r.invalid_payment_amount_count,

        -- Total payment issues for reliability scoring
        coalesce(r.missing_payment_count, 0)
        + coalesce(r.duplicate_payment_count, 0)
        + coalesce(r.failed_payment_on_completed_count, 0)
        + coalesce(r.invalid_payment_amount_count, 0)       as total_payment_issues,

        -- LTV per day since signup — normalises for rider tenure
        -- New riders with high early spend rank higher than old riders coasting
        case
            when coalesce(r.days_since_signup, 0) = 0 then null
            else round(
                coalesce(r.rider_lifetime_value, 0) / r.days_since_signup,
                4
            )
        end                                                 as ltv_per_day,

        -- Global LTV percentile rank — where does this rider sit
        -- across the full rider population?
        percent_rank() over (
            order by coalesce(r.rider_lifetime_value, 0)
        )                                                   as ltv_percentile,

        -- LTV value segment derived from percentile
        case
            when percent_rank() over (
                order by coalesce(r.rider_lifetime_value, 0)
            ) >= 0.9                                        then 'top_10_pct'
            when percent_rank() over (
                order by coalesce(r.rider_lifetime_value, 0)
            ) >= 0.75                                       then 'top_25_pct'
            when percent_rank() over (
                order by coalesce(r.rider_lifetime_value, 0)
            ) >= 0.5                                        then 'above_median'
            else                                                 'below_median'
        end                                                 as ltv_segment,

        -- Referral programme effectiveness:
        -- did referred riders spend more than organic riders?
        -- Populated for cohort comparison in downstream analysis
        case
            when r.is_referred_rider then 'referred'
            else 'organic'
        end                                                 as acquisition_cohort

    from {{ ref('dim_riders') }} as r

)

select
    -- Identity
    v.rider_id,
    v.country,
    v.signup_date,
    v.days_since_signup,
    v.referral_code,
    v.is_referred_rider,
    v.acquisition_cohort,
    v.rider_tier,

    -- Trip behaviour
    v.total_trips,
    v.completed_trips,
    v.cancelled_trips,
    v.no_show_trips,
    v.completion_rate_pct,
    v.cancellation_rate_pct,

    -- Revenue & LTV
    v.rider_lifetime_value,
    v.avg_spend_per_trip,
    v.ltv_per_day,
    v.ltv_percentile,
    v.ltv_segment,

    -- Preferred city (with name from dim_cities)
    v.most_frequent_city_id,
    c.city_name                                             as most_frequent_city_name,

    -- Surge & corporate behaviour
    v.extreme_surge_trips,
    v.corporate_trips,

    -- Payment integrity
    v.missing_payment_count,
    v.duplicate_payment_count,
    v.failed_payment_on_completed_count,
    v.invalid_payment_amount_count,
    v.total_payment_issues

from rider_valued as v
left join {{ ref('dim_cities') }} as c
    on v.most_frequent_city_id = c.city_id