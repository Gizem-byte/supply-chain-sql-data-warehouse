/*=====================================================================================
  GOLD LAYER – Profit Drivers Simulator (Business KPI Star Schema)
  Author: Gizem
  Purpose:
      - Create analytics-ready dimensional model for profitability analysis
      - Integrate Operational + Commercial + Profitability metrics
      - Feed interactive Profit Drivers Simulator dashboards
=====================================================================================*/

USE Supply_Chain_Datawarehouse;
GO


/*-----------------------------------------------------------
  1️⃣  SCHEMA CREATION
-----------------------------------------------------------*/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END;
GO


/*-----------------------------------------------------------
  2️⃣  DIM_DATE – Time dimension
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.dim_date AS
SELECT DISTINCT
    CAST(order_date AS DATE) AS date_key,
    YEAR(order_date) AS year_number,
    MONTH(order_date) AS month_number,
    DATEPART(QUARTER, order_date) AS quarter_number,
    DATENAME(MONTH, order_date) AS month_name,
    DATEPART(WEEK, order_date) AS week_number,
    DATENAME(WEEKDAY, order_date) AS weekday_name
FROM silver.orders
WHERE order_date IS NOT NULL;
GO


/*-----------------------------------------------------------
  3️⃣  DIM_CUSTOMER – Behavior & profitability segmentation
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.dim_customer AS
SELECT 
    c.customer_id             AS customer_key,
    c.customer_full_name,
    c.customer_city,
    c.customer_state,
    c.customer_country,
    c.types_of_customers      AS customer_segment,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(t.order_item_net_total) AS total_revenue,
    SUM(t.order_profit_per_order) AS total_profit,
    ROUND(SUM(t.order_profit_per_order)/NULLIF(SUM(t.order_item_net_total),0)*100,2) AS avg_margin_pct,
    CASE 
        WHEN COUNT(DISTINCT o.order_id) >= 10 THEN 'Loyal'
        WHEN COUNT(DISTINCT o.order_id) BETWEEN 5 AND 9 THEN 'Regular'
        ELSE 'New'
    END AS loyalty_tier
FROM silver.customer c
LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
LEFT JOIN silver.order_transaction t ON o.order_id = t.order_id
GROUP BY c.customer_id, c.customer_full_name, c.customer_city, c.customer_state, c.customer_country, c.types_of_customers;
GO


/*-----------------------------------------------------------
  4️⃣  DIM_PRODUCT – Profitability & discount metrics
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.dim_product AS
SELECT
    p.product_barcode_id       AS product_key,
    p.product_name,
    p.product_category_name,
    p.store_department_name,
    AVG(p.product_unit_price)  AS avg_price,
    SUM(t.order_item_quantity) AS total_units_sold,
    SUM(t.order_item_net_total) AS total_revenue,
    SUM(t.order_profit_per_order) AS total_profit,
    ROUND(SUM(t.order_profit_per_order)/NULLIF(SUM(t.order_item_net_total),0)*100,2) AS profit_margin_pct,
    ROUND(AVG(t.order_item_discount_percentage),2) AS avg_discount_pct
FROM silver.product p
LEFT JOIN silver.order_transaction t ON p.product_barcode_id = t.product_barcode_id
GROUP BY p.product_barcode_id, p.product_name, p.product_category_name, p.store_department_name;
GO


/*-----------------------------------------------------------
  5️⃣  DIM_SHIPPING – Operational delivery performance
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.dim_shipping AS
SELECT
    s.shipping_mode,
    COUNT(t.order_id) AS total_orders,
    SUM(CASE WHEN t.late_delivery_flag = 1 THEN 1 ELSE 0 END) AS late_orders,
    ROUND(SUM(CASE WHEN t.late_delivery_flag = 1 THEN 1 ELSE 0 END)*100.0/COUNT(t.order_id),2) AS late_delivery_rate_pct,
    ROUND(AVG(s.actual_shipping_days),2) AS avg_actual_shipping_days,
    ROUND(AVG(s.scheduled_delivery_days),2) AS avg_scheduled_days,
    ROUND(AVG(s.actual_shipping_days - s.scheduled_delivery_days),2) AS avg_delay_days,
    SUM(CASE WHEN o.order_status='Canceled' THEN 1 ELSE 0 END) AS canceled_orders,
    ROUND(SUM(CASE WHEN o.order_status='Canceled' THEN 1 ELSE 0 END)*100.0/COUNT(t.order_id),2) AS cancellation_rate_pct
FROM silver.shipping s
LEFT JOIN silver.order_transaction t ON s.shipping_mode = t.shipping_mode
LEFT JOIN silver.orders o ON o.order_id = t.order_id
GROUP BY s.shipping_mode;
GO


/*-----------------------------------------------------------
  6️⃣  DIM_MARKET – Geography dimension
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.dim_market AS
SELECT DISTINCT
    o.order_continent AS market_key,
    o.order_country,
    o.order_subregion,
    o.order_state,
    o.order_city
FROM silver.orders o
WHERE o.order_continent IS NOT NULL;
GO


/*-----------------------------------------------------------
  7️⃣  FACT_PROFITABILITY – Unified fact table for analysis
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.fact_profitability AS
SELECT 
    t.order_item_id              AS fact_key,
    o.order_id                   AS order_key,
    o.customer_id                AS customer_key,
    t.product_barcode_id         AS product_key,
    t.shipping_mode,
    o.order_date                 AS order_date_key,
    o.order_country,
    o.order_continent,
    -- 💰 Financial Metrics
    t.order_item_quantity,
    t.order_item_net_total       AS revenue,
    t.order_profit_per_order     AS profit,
    ROUND((t.order_profit_per_order / NULLIF(t.order_item_net_total,0))*100,2) AS profit_margin_pct,
    t.order_item_discount_percentage AS discount_rate,
    t.order_item_discount         AS discount_amount,
    ROUND(t.order_item_net_total / NULLIF(t.order_item_quantity,0),2) AS avg_price_per_item,
    SUM(t.order_item_net_total) OVER (PARTITION BY o.order_id) AS basket_value_eur,
    -- 🚚 Operational Metrics
    s.scheduled_delivery_days,
    s.actual_shipping_days,
    (s.actual_shipping_days - s.scheduled_delivery_days) AS delivery_delay_days,
    CASE WHEN s.actual_shipping_days > s.scheduled_delivery_days THEN 1 ELSE 0 END AS late_flag,
    CASE WHEN o.order_status = 'Canceled' THEN 1 ELSE 0 END AS canceled_flag,
    -- 🧮 Derived Composite Metrics
    CASE 
        WHEN t.order_item_discount_percentage > 20 THEN 'High Discount'
        WHEN t.order_item_discount_percentage BETWEEN 10 AND 20 THEN 'Medium Discount'
        ELSE 'Low Discount'
    END AS discount_band,
    CASE 
        WHEN s.actual_shipping_days > s.scheduled_delivery_days AND t.order_item_discount_percentage > 15 THEN 'Late & High Discount'
        WHEN s.actual_shipping_days > s.scheduled_delivery_days THEN 'Late Delivery'
        WHEN t.order_item_discount_percentage > 15 THEN 'High Discount Only'
        ELSE 'Healthy Order'
    END AS risk_category,
    SYSDATETIME() AS ingestion_timestamp
FROM silver.order_transaction t
JOIN silver.orders o      ON t.order_id = o.order_id
LEFT JOIN silver.customer c ON o.customer_id = c.customer_id
LEFT JOIN silver.shipping s ON t.shipping_mode = s.shipping_mode;
GO


/*-----------------------------------------------------------
  8️⃣  PROFIT_DRIVERS – Aggregated KPI table for Simulator
     🔧 FIXED: replaced m.market_key with f.order_continent
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.profit_drivers AS
SELECT 
    f.order_continent AS market_key,
    f.shipping_mode,
    COUNT(DISTINCT f.order_key) AS total_orders,
    SUM(f.revenue) AS total_revenue,
    SUM(f.profit) AS total_profit,
    ROUND(SUM(f.profit)/NULLIF(SUM(f.revenue),0)*100,2) AS profit_margin_pct,
    ROUND(AVG(f.discount_rate),2) AS avg_discount_pct,
    ROUND(AVG(f.basket_value_eur),2) AS avg_basket_size_eur,
    SUM(CASE WHEN f.late_flag=1 THEN 1 ELSE 0 END)*100.0/COUNT(f.order_key) AS late_delivery_rate_pct,
    SUM(CASE WHEN f.canceled_flag=1 THEN 1 ELSE 0 END)*100.0/COUNT(f.order_key) AS cancellation_rate_pct,
    -- Combined operational + commercial efficiency
    ROUND(SUM(f.profit)/NULLIF(SUM(f.basket_value_eur),0)*100,2) AS profit_per_basket_pct
FROM gold.fact_profitability f
GROUP BY f.order_continent, f.shipping_mode;
GO


/*-----------------------------------------------------------
  ✅ COMPLETION MESSAGE
-----------------------------------------------------------*/
PRINT '-----------------------------------------------------------';
PRINT '✅ GOLD LAYER CREATED SUCCESSFULLY (FACT + 5 DIMS + PROFIT DRIVERS)';
PRINT '-----------------------------------------------------------';
GO
