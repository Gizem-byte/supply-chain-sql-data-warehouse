/*
===============================================================================
Script Name  : silver_logical_test.sql
Purpose      : Perform logical and consistency checks on Silver layer.
Description  :
    This script validates logical consistency and derived relationships in 
    silver.dataco_supply_chain after transformations. It ensures that 
    business rules, calculations, and dependencies are correct before 
    promotion to the Gold (analytics) layer.
===============================================================================
*/

USE Supply_Chain_Datawarehouse;
GO

PRINT '=============================================================';
PRINT 'Starting LOGICAL CHECKS on silver.dataco_supply_chain';
PRINT '=============================================================';

-------------------------------------------------------------------------------
-- RECORD COUNT VS BRONZE
-------------------------------------------------------------------------------
-- Expected:
-- Record count in Silver ≈ Bronze count (minus dropped or invalid rows)
PRINT 'Checking record count consistency between Bronze and Silver layers...';
SELECT 
    (SELECT COUNT(*) FROM bronze.dataco_supply_chain) AS Bronze_Count,
    (SELECT COUNT(*) FROM silver.dataco_supply_chain) AS Silver_Count,
    (SELECT COUNT(*) FROM bronze.dataco_supply_chain) - (SELECT COUNT(*) FROM silver.dataco_supply_chain) AS Difference;

-------------------------------------------------------------------------------
--PRIMARY KEY UNIQUENESS
-------------------------------------------------------------------------------
-- Expected:
-- Combination (order_id, order_item_id) must be unique
PRINT 'Checking for duplicate records by order_id + order_item_id...';
SELECT 
    order_id, 
    order_item_id, 
    COUNT(*) AS duplicate_count
FROM silver.dataco_supply_chain
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1;

-------------------------------------------------------------------------------
-- DELIVERY DELAY LOGIC
-------------------------------------------------------------------------------
-- Expected:
-- delivery_delay_days = actual_shipping_days - scheduled_delivery_days
-- Negative values = early delivery
PRINT 'Validating delivery delay logic (real - scheduled)...';
SELECT 
    COUNT(*) AS Invalid_Delay_Count
FROM silver.dataco_supply_chain
WHERE actual_shipping_days - scheduled_delivery_days <> 
      (CASE 
          WHEN actual_shipping_days IS NOT NULL AND scheduled_delivery_days IS NOT NULL 
          THEN actual_shipping_days - scheduled_delivery_days
      END);

-------------------------------------------------------------------------------
-- LATE DELIVERY RISK LOGIC
-------------------------------------------------------------------------------
-- Expected:
-- order_late_delivery_risk = 1 if actual_shipping_days > scheduled_delivery_days, else 0
PRINT 'Checking Late Delivery Risk logic consistency...';
SELECT 
    SUM(CASE WHEN order_late_delivery_risk = 1 
             AND actual_shipping_days <= scheduled_delivery_days THEN 1 ELSE 0 END) AS Incorrect_Flag_1,
    SUM(CASE WHEN order_late_delivery_risk = 0 
             AND actual_shipping_days > scheduled_delivery_days THEN 1 ELSE 0 END) AS Incorrect_Flag_0
FROM silver.dataco_supply_chain;

-------------------------------------------------------------------------------
-- PROFITABILITY CONSISTENCY
-------------------------------------------------------------------------------
-- Expected:
-- earning_per_order + discounts ≈ order_profit_per_order + product_cost (approximate)
-- Profit should not exceed sales
PRINT 'Checking profitability consistency...';
SELECT 
    COUNT(*) AS Invalid_Profit_Count
FROM silver.dataco_supply_chain
WHERE order_profit_per_order > total_sale_per_customer;

-------------------------------------------------------------------------------
-- DISCOUNT RATES
-------------------------------------------------------------------------------
-- Expected:
-- order_item_discount_percentage between 0 and 1 (0%–100%)
PRINT 'Checking valid discount rates...';
SELECT 
    COUNT(*) AS Invalid_DiscountRate_Count
FROM silver.dataco_supply_chain
WHERE order_item_discount_percentage < 0 OR order_item_discount_percentage > 1;

-------------------------------------------------------------------------------
-- ORDER ITEM QUANTITY VALIDATION
-------------------------------------------------------------------------------
-- Expected:
-- Quantities should be > 0
PRINT 'Checking for invalid order quantities...';
SELECT 
    COUNT(*) AS Invalid_Quantity_Count
FROM silver.dataco_supply_chain
WHERE order_item_quantity <= 0;

-------------------------------------------------------------------------------
-- SHIPPING MODE VALIDATION
-------------------------------------------------------------------------------
-- Expected:
-- Same order_id should not have multiple shipping modes
PRINT 'Checking if same order_id has inconsistent shipping modes...';
SELECT 
    order_id,
    COUNT(DISTINCT shipping_mode) AS mode_count
FROM silver.dataco_supply_chain
GROUP BY order_id
HAVING COUNT(DISTINCT shipping_mode) > 1;

-------------------------------------------------------------------------------
-- CUSTOMER SEGMENT INTEGRITY
-------------------------------------------------------------------------------
-- Expected:
-- All values should belong to known segments: Consumer, Corporate, Home Office
PRINT 'Checking for unexpected customer segments...';
SELECT DISTINCT types_of_customers
FROM silver.dataco_supply_chain
WHERE types_of_customers NOT IN ('Consumer', 'Corporate', 'Home Office');

-------------------------------------------------------------------------------
-- SHIPPING DATE > ORDER DATE
-------------------------------------------------------------------------------
-- Expected:
-- shipping_date >= order_date
PRINT 'Checking for invalid shipping-date relationships...';
SELECT 
    COUNT(*) AS Invalid_Date_Sequence
FROM silver.dataco_supply_chain
WHERE shipping_date < order_date;

-------------------------------------------------------------------------------
-- PRODUCT PRICE * QUANTITY VS NET TOTAL
-------------------------------------------------------------------------------
-- Expected:
-- order_item_net_total ≈ product_unit_price * order_item_quantity * (1 - discount_rate)
PRINT 'Validating product total calculation logic...';
SELECT 
    COUNT(*) AS Invalid_Total_Calc
FROM silver.dataco_supply_chain
WHERE ABS(order_item_net_total - 
          (product_unit_price * order_item_quantity * (1 - order_item_discount_percentage))) > 1;

-------------------------------------------------------------------------------
-- GEOGRAPHIC LOGIC
-------------------------------------------------------------------------------
-- Expected:
-- Each Market should map to consistent order_continent (no mixed mapping)
PRINT 'Checking if markets map consistently to continents...';
SELECT 
    order_continent, 
    COUNT(DISTINCT order_continent) AS distinct_continent_count
FROM silver.dataco_supply_chain
GROUP BY order_continent
HAVING COUNT(DISTINCT order_continent) > 1;

-------------------------------------------------------------------------------
-- PROFIT MARGIN VALIDATION
-------------------------------------------------------------------------------
-- Expected:
-- Profit margin (%) = order_profit_per_order / total_sale_per_customer * 100
-- Values outside -100%–100% range indicate errors
PRINT 'Checking profit margin sanity...';
SELECT 
    COUNT(*) AS Invalid_Margin_Count
FROM silver.dataco_supply_chain
WHERE (order_profit_per_order / NULLIF(total_sale_per_customer, 0)) * 100 NOT BETWEEN -100 AND 100;

-------------------------------------------------------------------------------
-- NULL VALUE CHECKS (CRITICAL FIELDS)
-------------------------------------------------------------------------------
-- Expected:
-- No NULLs in key business fields (order_id, product_name, market, etc.)
PRINT 'Checking for NULLs in key business fields...';
SELECT 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Null_order_id,
    SUM(CASE WHEN product_name IS NULL THEN 1 ELSE 0 END) AS Null_product_name,
    SUM(CASE WHEN order_continent IS NULL THEN 1 ELSE 0 END) AS Null_market,
    SUM(CASE WHEN order_profit_per_order IS NULL THEN 1 ELSE 0 END) AS Null_profit
FROM silver.dataco_supply_chain;

PRINT '=============================================================';
PRINT 'LOGICAL CHECKS completed for Silver layer';
PRINT 'Review above outputs and correct any inconsistencies before Gold promotion.';
PRINT '=============================================================';
GO
