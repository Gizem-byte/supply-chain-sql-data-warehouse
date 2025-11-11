/*=====================================================================
  GOLD LAYER – Final Star Schema for Profitability Simulator Project
  Author: Gizem
  Purpose:
    - Transform Silver layer into analytics-ready star schema
    - Clean separation: descriptive (dim) vs measurable (fact)
=====================================================================*/

USE Supply_Chain_Datawarehouse;
GO

/*-----------------------------------------------------------
    SCHEMA CREATION
-----------------------------------------------------------*/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO


/*-----------------------------------------------------------
    DIM: DATE  (includes quarter for time-series analysis)
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_dim_date AS
SELECT DISTINCT
    CAST(order_date AS DATE) AS date_key,
    YEAR(order_date) AS year_number,
    MONTH(order_date) AS month_number,
    DATEPART(QUARTER, order_date) AS quarter_number,
    DATENAME(MONTH, order_date) AS month_name,
    DATEPART(WEEK, order_date) AS week_number,
    DATENAME(WEEKDAY, order_date) AS weekday_name
FROM silver.dataco_supply_chain
WHERE order_date IS NOT NULL;
GO


/*-----------------------------------------------------------
    DIM: CUSTOMER
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_dim_customer AS
SELECT DISTINCT
    customer_id AS customer_key,
    customer_full_name,
    customer_city,
    customer_country,
    customer_state,
    types_of_customers AS customer_segment
FROM silver.dataco_supply_chain
WHERE customer_id IS NOT NULL;
GO


/*-----------------------------------------------------------
    DIM: PRODUCT
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_dim_product AS
SELECT DISTINCT
    product_barcode_id AS product_key,
    product_name,
    product_category_id,
    product_category_name,
    store_department_id,
    store_department_name
FROM silver.dataco_supply_chain
WHERE product_barcode_id IS NOT NULL;
GO


/*-----------------------------------------------------------
    DIM: MARKET
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_dim_market AS
SELECT DISTINCT
    order_continent AS market_key,
    order_country,
    order_subregion,
    order_state,
    order_city
FROM silver.dataco_supply_chain
WHERE order_continent IS NOT NULL;
GO


/*-----------------------------------------------------------
    DIM: ORDER
    Includes shipping attributes & operational performance flags
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_dim_order AS
SELECT DISTINCT
    order_id AS order_key,
    payment_type,
    order_status,
    order_delivery_status,
    shipping_mode,
    CASE 
        WHEN shipping_mode LIKE '%Same Day%' THEN 'Fast'
        WHEN shipping_mode LIKE '%First%' OR shipping_mode LIKE '%Express%' THEN 'Priority'
        ELSE 'Standard'
    END AS shipping_speed_category,
    CASE 
        WHEN shipping_mode LIKE '%Express%' OR shipping_mode LIKE '%First%' THEN 1 ELSE 0
    END AS is_premium_mode,
    TRY_CAST(actual_shipping_days AS INT) AS actual_shipping_days,
    TRY_CAST(scheduled_delivery_days AS INT) AS scheduled_delivery_days,
    (NULLIF(actual_shipping_days,0) - NULLIF(scheduled_delivery_days,0)) AS delivery_delay_days,
    CASE WHEN (NULLIF(actual_shipping_days,0) > NULLIF(scheduled_delivery_days,0)) THEN 1 ELSE 0 END AS is_late,
    CASE WHEN order_delivery_status = 'Canceled' THEN 1 ELSE 0 END AS canceled_flag,
    order_date AS order_date_key,
    shipping_date AS shipping_date_key
FROM silver.dataco_supply_chain
WHERE order_id IS NOT NULL;
GO


/*-----------------------------------------------------------
    FACT: SALES / PROFITABILITY
    All measurable, numeric, transaction-level KPIs
-----------------------------------------------------------*/
CREATE OR ALTER VIEW gold.vw_fact_sales AS
SELECT 
    s.order_item_id            AS sales_key,
    s.order_id                 AS order_key,
    s.product_barcode_id       AS product_key,
    s.customer_id              AS customer_key,
    s.order_continent          AS market_key,

    -- 🔹 Transactional metrics (measurable)
    ROUND(s.product_unit_price, 1)       AS product_unit_price,
    ROUND(s.order_item_quantity, 1)      AS order_item_quantity,
    ROUND(s.order_item_discount, 1)      AS order_item_discount,
    ROUND(s.order_item_discount_percentage, 2) AS order_item_discount_percentage,
    ROUND(s.order_item_gross_total, 1)   AS order_item_gross_total,
    ROUND(s.order_item_net_total, 1)     AS order_item_net_total,
    ROUND(s.order_profit_per_order, 1)   AS order_profit_per_order,
    ROUND(s.earning_per_order, 1)        AS earning_per_order,
    ROUND(s.total_sale_per_customer, 1)  AS total_sale_per_customer,

    -- 🔹 Derived KPIs
    CASE 
        WHEN s.total_sale_per_customer <> 0 
             THEN ROUND((s.order_profit_per_order / s.total_sale_per_customer) * 100, 2)
        ELSE NULL 
    END AS profit_margin_percentage,

    s.order_late_delivery_risk AS late_delivery_risk_flag,
    SYSDATETIME() AS ingestion_timestamp
FROM silver.dataco_supply_chain s
WHERE s.order_id IS NOT NULL;
GO


/*-----------------------------------------------------------
    VERIFICATION (quick sanity check)
-----------------------------------------------------------*/
PRINT '-----------------------------------------------------------';
PRINT '✅ GOLD LAYER CREATED SUCCESSFULLY (FACT + 4 DIMENSIONS)';
PRINT '-----------------------------------------------------------';
SELECT 
    'gold.vw_dim_date' AS view_name, COUNT(*) AS rows FROM gold.vw_dim_date UNION ALL
SELECT 'gold.vw_dim_customer', COUNT(*) FROM gold.vw_dim_customer UNION ALL
SELECT 'gold.vw_dim_product', COUNT(*) FROM gold.vw_dim_product UNION ALL
SELECT 'gold.vw_dim_market', COUNT(*) FROM gold.vw_dim_market UNION ALL
SELECT 'gold.vw_dim_order', COUNT(*) FROM gold.vw_dim_order UNION ALL
SELECT 'gold.vw_fact_sales', COUNT(*) FROM gold.vw_fact_sales;
GO
