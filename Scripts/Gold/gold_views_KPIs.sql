USE Supply_Chain_Datawarehouse;
GO

/*===============================================================================
  1. OPERATIONAL EFFICIENCY KPIs
     - Shipping times, delays, late rate, cancellations, lost profit
===============================================================================*/

IF OBJECT_ID('gold.v_operational_kpis', 'V') IS NOT NULL
    DROP VIEW gold.v_operational_kpis;
GO

CREATE VIEW gold.v_operational_kpis AS
SELECT
    dl.order_continent                             AS market,
    dl.order_country,
    dl.order_subregion,
    fo.shipping_mode,

    COUNT(DISTINCT fo.order_id)                    AS total_orders,

    -- core shipping metrics
    AVG(CAST(fo.actual_shipping_days AS DECIMAL(18,4)))
                                                   AS avg_actual_shipping_days,
    AVG(CAST(fo.scheduled_shipping_days AS DECIMAL(18,4)))
                                                   AS avg_scheduled_shipping_days,
    AVG(CAST(fo.delivery_delay_days AS DECIMAL(18,4)))
                                                   AS avg_delivery_delay_days,

    -- late delivery rate (% orders flagged as late)
    100.0 * SUM(CASE WHEN fo.order_late_delivery_risk = 1 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                    AS late_delivery_rate_pct,

    -- cancellation rate (% orders canceled)
    100.0 * SUM(CASE WHEN fo.order_delivery_status = 'Canceled' 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                    AS cancellation_rate_pct,

    -- on-time delivery ratio
    100.0 * SUM(CASE WHEN fo.actual_shipping_days IS NOT NULL
                          AND fo.scheduled_shipping_days IS NOT NULL
                          AND fo.actual_shipping_days 
                              <= fo.scheduled_shipping_days
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                    AS on_time_ratio_pct,

    -- lost profit from cancellations
    SUM(CASE WHEN fo.order_delivery_status = 'Canceled' 
             THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
             ELSE 0 END)                           AS lost_profit_from_cancellations

FROM gold.fact_orders fo
LEFT JOIN gold.dim_order_location dl
       ON fo.order_location_key = dl.order_location_key
GROUP BY
    dl.order_continent,
    dl.order_country,
    dl.order_subregion,
    fo.shipping_mode;
GO


/*===============================================================================
  2. COMMERCIAL PERFORMANCE KPIs
     - Discount, basket size, quantity, price, margin, high-discount share
===============================================================================*/

IF OBJECT_ID('gold.v_commercial_kpis', 'V') IS NOT NULL
    DROP VIEW gold.v_commercial_kpis;
GO

CREATE VIEW gold.v_commercial_kpis AS
SELECT
    dl.order_continent                              AS market,
    dl.order_country,
    dl.order_subregion,
    dp.product_category_name,
    dp.store_department_name,

    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    COUNT(*)                                        AS total_order_items,

    -- discounts
    AVG(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
                                                    AS avg_discount_rate,
    MIN(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
                                                    AS min_discount_rate,
    MAX(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
                                                    AS max_discount_rate,
    100.0 * SUM(CASE WHEN oi.order_item_discount_percentage > 0.5 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                     AS high_discount_items_share_pct,

    -- basket size (per order/customer)
    AVG(CAST(fo.total_sale_per_customer AS DECIMAL(18,4)))
                                                    AS avg_basket_size,

    -- quantity & price
    AVG(CAST(oi.order_item_quantity AS DECIMAL(18,4)))
                                                    AS avg_order_item_quantity,
    AVG(CAST(oi.product_unit_price AS DECIMAL(18,4)))
                                                    AS avg_product_price,

    -- profit margin (%)
    AVG(
        CASE 
            WHEN fo.total_sale_per_customer > 0 
                THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
                     / CAST(fo.total_sale_per_customer AS DECIMAL(18,4)) * 100.0
            ELSE NULL
        END
    )                                               AS avg_profit_margin_pct,

    -- total profit contributed by this market/category
    SUM(CAST(fo.order_profit_per_order AS DECIMAL(18,4)))
                                                    AS total_profit

FROM gold.fact_order_items oi
JOIN gold.fact_orders fo
      ON oi.order_key = fo.order_key
LEFT JOIN gold.dim_order_location dl
       ON fo.order_location_key = dl.order_location_key
LEFT JOIN gold.dim_product dp
       ON oi.product_key = dp.product_key
GROUP BY
    dl.order_continent,
    dl.order_country,
    dl.order_subregion,
    dp.product_category_name,
    dp.store_department_name;
GO


/*===============================================================================
  3. PROFITABILITY KPIs
     - Total profit, profit by market/mode, late vs on-time gap,
       cancellation revenue loss
===============================================================================*/

IF OBJECT_ID('gold.v_profitability_kpis', 'V') IS NOT NULL
    DROP VIEW gold.v_profitability_kpis;
GO

CREATE VIEW gold.v_profitability_kpis AS
SELECT
    dl.order_continent                              AS market,
    dl.order_country,
    dl.order_subregion,
    fo.shipping_mode,

    COUNT(DISTINCT fo.order_id)                     AS total_orders,

    -- profit metrics
    SUM(CAST(fo.order_profit_per_order AS DECIMAL(18,4)))
                                                    AS total_profit,
    AVG(CAST(fo.order_profit_per_order AS DECIMAL(18,4)))
                                                    AS avg_profit_per_order,
    AVG(CAST(fo.order_profit_margin AS DECIMAL(18,4)))
                                                    AS avg_profit_margin_pct,

    -- profit by late vs on-time
    AVG(
        CASE WHEN fo.order_late_delivery_risk = 1 
             THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
        END
    )                                               AS avg_profit_late_orders,
    AVG(
        CASE WHEN fo.order_late_delivery_risk = 0 
             THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
        END
    )                                               AS avg_profit_ontime_orders,

    AVG(
        CASE WHEN fo.order_late_delivery_risk = 1 
             THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
        END
    ) -
    AVG(
        CASE WHEN fo.order_late_delivery_risk = 0 
             THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
        END
    )                                               AS late_vs_ontime_profit_gap,

    -- cancellation revenue loss %
    100.0 * SUM(
                CASE WHEN fo.order_delivery_status = 'Canceled' 
                     THEN CAST(fo.order_profit_per_order AS DECIMAL(18,4))
                     ELSE 0 END
          )
          / NULLIF(SUM(CAST(fo.order_profit_per_order AS DECIMAL(18,4))), 0)
                                                    AS cancellation_revenue_loss_pct,

    -- late / cancellation rates for integration view
    100.0 * SUM(CASE WHEN fo.order_late_delivery_risk = 1 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                     AS late_delivery_rate_pct,
    100.0 * SUM(CASE WHEN fo.order_delivery_status = 'Canceled' 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                     AS cancellation_rate_pct

FROM gold.fact_orders fo
LEFT JOIN gold.dim_order_location dl
       ON fo.order_location_key = dl.order_location_key
GROUP BY
    dl.order_continent,
    dl.order_country,
    dl.order_subregion,
    fo.shipping_mode;
GO


/*===============================================================================
  4. PROFITABILITY INTEGRATION KPIs
     - Discount buckets, market profit rank, basic integration fields
===============================================================================*/

IF OBJECT_ID('gold.v_profit_integration_kpis', 'V') IS NOT NULL
    DROP VIEW gold.v_profit_integration_kpis;
GO

CREATE VIEW gold.v_profit_integration_kpis AS
WITH base AS (
    SELECT
        dl.order_continent                      AS market,
        fo.shipping_mode,
        fo.order_id,
        fo.order_profit_per_order,
        fo.order_profit_margin,
        fo.order_late_delivery_risk,
        fo.order_delivery_status,
        fo.total_sale_per_customer,
        AVG(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
            OVER (PARTITION BY fo.order_id)     AS order_avg_discount_rate
    FROM gold.fact_orders fo
    LEFT JOIN gold.fact_order_items oi
           ON fo.order_key = oi.order_key
    LEFT JOIN gold.dim_order_location dl
           ON fo.order_location_key = dl.order_location_key
)
SELECT
    market,
    shipping_mode,

    COUNT(DISTINCT order_id)                     AS total_orders,
    SUM(CAST(order_profit_per_order AS DECIMAL(18,4)))
                                                AS total_profit,
    AVG(CAST(order_profit_per_order AS DECIMAL(18,4)))
                                                AS avg_profit_per_order,
    AVG(CAST(order_profit_margin AS DECIMAL(18,4)))
                                                AS avg_profit_margin_pct,

    -- discount metrics
    AVG(order_avg_discount_rate)                AS avg_discount_rate,
    100.0 * SUM(CASE WHEN order_avg_discount_rate > 0.5 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                 AS high_discount_order_share_pct,

    -- basket size
    AVG(CAST(total_sale_per_customer AS DECIMAL(18,4)))
                                                AS avg_basket_size,

    -- late & cancellation
    100.0 * SUM(CASE WHEN order_late_delivery_risk = 1 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                 AS late_rate_pct,
    100.0 * SUM(CASE WHEN order_delivery_status = 'Canceled' 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                 AS cancellation_rate_pct

FROM base
GROUP BY
    market,
    shipping_mode;
GO


/*===============================================================================
  5. GLOBAL CORRELATION-LIKE SUMMARY VIEW
     (overall efficiency–profit / discount–profit / basket–profit relationships)
===============================================================================*/

IF OBJECT_ID('gold.v_correlation_kpis', 'V') IS NOT NULL
    DROP VIEW gold.v_correlation_kpis;
GO

CREATE VIEW gold.v_correlation_kpis AS
WITH base AS (
    SELECT
        fo.order_id,
        fo.order_profit_per_order,
        fo.order_late_delivery_risk,
        fo.total_sale_per_customer,
        AVG(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
            OVER (PARTITION BY fo.order_id) AS order_avg_discount_rate
    FROM gold.fact_orders fo
    LEFT JOIN gold.fact_order_items oi
           ON fo.order_key = oi.order_key
)
SELECT
    COUNT(*) AS n_orders,

    -- simple aggregates to inspect relationships (can be used in Tableau for scatter plots)
    AVG(CAST(order_late_delivery_risk AS DECIMAL(18,4)))
                                            AS avg_late_flag,
    AVG(CAST(order_profit_per_order AS DECIMAL(18,4)))
                                            AS avg_profit,
    AVG(order_avg_discount_rate)           AS avg_discount_rate,
    AVG(CAST(total_sale_per_customer AS DECIMAL(18,4)))
                                            AS avg_basket_size

    -- You can compute Pearson correlation in Tableau or a separate SQL view if needed,
    -- using these base columns (late flag vs profit, discount vs profit, basket vs profit).

FROM base;
GO


/*===============================================================================
  6. BASELINE METRICS FOR SIMULATOR
     - Single-row snapshot of key KPIs for what-if scenarios
===============================================================================*/

IF OBJECT_ID('gold.v_baseline_metrics', 'V') IS NOT NULL
    DROP VIEW gold.v_baseline_metrics;
GO

CREATE VIEW gold.v_baseline_metrics AS
WITH base AS (
    SELECT
        fo.*,
        AVG(CAST(oi.order_item_discount_percentage AS DECIMAL(18,4)))
            OVER (PARTITION BY fo.order_id) AS order_avg_discount_rate
    FROM gold.fact_orders fo
    LEFT JOIN gold.fact_order_items oi
           ON fo.order_key = oi.order_key
)
SELECT
    COUNT(*)                                      AS total_orders,
    SUM(CAST(order_profit_per_order AS DECIMAL(18,4)))
                                                  AS total_profit,
    AVG(CAST(order_profit_per_order AS DECIMAL(18,4)))
                                                  AS avg_profit_per_order,
    AVG(CAST(order_profit_margin AS DECIMAL(18,4)))
                                                  AS avg_profit_margin_pct,
    AVG(CAST(total_sale_per_customer AS DECIMAL(18,4)))
                                                  AS avg_basket_size,

    -- operational baseline
    100.0 * SUM(CASE WHEN order_late_delivery_risk = 1 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                  AS late_delivery_rate_pct,
    100.0 * SUM(CASE WHEN order_delivery_status = 'Canceled' 
                     THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0)                  AS cancellation_rate_pct,
    AVG(CAST(actual_shipping_days AS DECIMAL(18,4)))
                                                  AS avg_actual_shipping_days,
    AVG(CAST(scheduled_shipping_days AS DECIMAL(18,4)))
                                                  AS avg_scheduled_shipping_days,
    AVG(CAST(delivery_delay_days AS DECIMAL(18,4)))
                                                  AS avg_delivery_delay_days,

    -- commercial baseline
    AVG(order_avg_discount_rate)                 AS avg_discount_rate

FROM base;
GO
