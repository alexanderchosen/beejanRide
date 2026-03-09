-- Grain: 1 row per city per day
-- TAGS: finance
-- OWNER: finance team



with daily_trips as (
    select
        f.trip_date, f.city_id,
        -- Trip volume
        count(*) as total_trips,
        count(case when f.status = 'completed' then 1 end) as completed_trips,
        count(case when f.status = 'cancelled' then 1 end) as cancelled_trips,
        count(case when f.status = 'no_show' then 1 end) as no_show_trips,

        -- Revenue
        sum(case when f.status = 'completed'
            then f.net_revenue else 0 end) as total_net_revenue,

        -- Corporate vs consumer split
        sum(case when f.corporate_trip_flag = 1
            then f.net_revenue else 0 end) as corporate_net_revenue,
        sum(case when f.corporate_trip_flag = 0
            then f.net_revenue else 0 end) as consumer_net_revenue,
        count(case when f.corporate_trip_flag = 1 then 1 end)  as corporate_trips,

        -- Surge revenue signal
        sum(case when f.extreme_surge_flag = 1
            then f.net_revenue else 0 end) as extreme_surge_revenue,

        -- Payment method breakdown
        count(case when f.payment_method = 'card' then 1 end)  as card_trips,
        count(case when f.payment_method = 'wallet' then 1 end) as wallet_trips,
        count(case when f.payment_method = 'cash' then 1 end)  as cash_trips,

        -- Payment integrity signals for finance review
        count(case when f.missing_payment_flag = 1 then 1 end) as missing_payment_trips,
        count(case when f.failed_payment_on_completed_trip_flag = 1
            then 1 end) as failed_payment_on_completed_trips

    from {{ ref('fact_trips') }} as f

--    {% if is_incremental() %}
 --       where f.trip_date > (
--            select max(trip_date)
 --           from {{ this }}
--        )
--    {% endif %}

    group by f.trip_date, f.city_id

)

select
    -- Keys
    d.trip_date, d.city_id, c.city_name, c.country,

    -- Trip volume
    d.total_trips, d.completed_trips, d.cancelled_trips, d.no_show_trips,

    -- Revenue
    d.total_net_revenue,

    -- Corporate vs consumer
    d.corporate_net_revenue, d.consumer_net_revenue, d.corporate_trips,

    -- Surge
    d.extreme_surge_revenue,

    -- Payment method breakdown
    d.card_trips, d.wallet_trips, d.cash_trips,

    -- Payment integrity
    d.missing_payment_trips, d.failed_payment_on_completed_trips

from daily_trips as d
left join {{ ref('dim_cities') }} as c
    on d.city_id = c.city_id