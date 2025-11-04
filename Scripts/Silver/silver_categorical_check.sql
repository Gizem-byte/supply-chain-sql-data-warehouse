/*
===============================================================================
Script Name  : categorical_check_silver.sql
Purpose      : Validate categorical and text fields in Silver layer for 
               consistency, typos, encoding issues, and unexpected values.
Author       : Gizem
===============================================================================
*/

USE Supply_Chain_Datawarehouse;
GO

PRINT '=============================================================';
PRINT 'Starting FULL CATEGORICAL VALIDATION for silver.dataco_supply_chain';
PRINT '=============================================================';

-------------------------------------------------------------------------------
-- 1DELIVERY STATUS
-------------------------------------------------------------------------------
PRINT 'Checking DELIVERY STATUS consistency...';
SELECT DISTINCT order_delivery_status
FROM silver.dataco_supply_chain
ORDER BY order_delivery_status;

PRINT 'Flag unexpected or misspelled values...';
SELECT DISTINCT order_delivery_status
FROM silver.dataco_supply_chain
WHERE order_delivery_status NOT IN (
    'Delivered', 'Canceled', 'Processing', 'Shipping', 'Late Delivery', 'Shipped'
);

-------------------------------------------------------------------------------
-- SHIPPING MODE
-------------------------------------------------------------------------------
PRINT 'Checking SHIPPING MODE...';
SELECT DISTINCT shipping_mode FROM silver.dataco_supply_chain ORDER BY shipping_mode;

PRINT 'Flag invalid shipping modes...';
SELECT DISTINCT shipping_mode
FROM silver.dataco_supply_chain
WHERE shipping_mode NOT IN ('Standard Class', 'Second Class', 'First Class', 'Same Day');

-------------------------------------------------------------------------------
-- CUSTOMER SEGMENT
-------------------------------------------------------------------------------
PRINT 'Checking CUSTOMER SEGMENTS...';
SELECT DISTINCT types_of_customers FROM silver.dataco_supply_chain ORDER BY types_of_customers;

PRINT 'Flag unexpected segments...';
SELECT DISTINCT types_of_customers
FROM silver.dataco_supply_chain
WHERE types_of_customers NOT IN ('Consumer', 'Corporate', 'Home Office');

-------------------------------------------------------------------------------
-- MARKET
-------------------------------------------------------------------------------
PRINT 'Checking MARKET...';
SELECT DISTINCT order_continent FROM silver.dataco_supply_chain ORDER BY order_continent;

PRINT 'Flag non-standard market values...';
SELECT DISTINCT order_continent
FROM silver.dataco_supply_chain
WHERE order_continent NOT IN ('LATAM','USCA','Europe','APAC','MEA','Africa');

-------------------------------------------------------------------------------
-- ORDER COUNTRY & CITY (Encoding / Typo Checks)
-------------------------------------------------------------------------------
PRINT 'Checking ORDER COUNTRY for encoding or spelling issues...';
SELECT DISTINCT order_country
FROM silver.dataco_supply_chain
WHERE order_country LIKE '%=%' OR order_country LIKE '%ß%' OR order_country LIKE '%ñ%' 
   OR order_country LIKE '%Ã%' OR order_country LIKE '%¤%' OR order_country LIKE '%~%'
   OR order_country LIKE '%#%' OR LEN(order_country) < 3
ORDER BY order_country;

PRINT 'Checking ORDER CITY for corrupted characters...';
SELECT DISTINCT order_city
FROM silver.dataco_supply_chain
WHERE order_city LIKE '%=%' OR order_city LIKE '%ß%' OR order_city LIKE '%¤%' 
   OR order_city LIKE '%Ã%' OR order_city LIKE '%~%' OR order_city LIKE '%#%'
ORDER BY order_city;

-------------------------------------------------------------------------------
-- CUSTOMER COUNTRY & CITY
-------------------------------------------------------------------------------
PRINT 'Checking CUSTOMER COUNTRY inconsistencies...';
SELECT DISTINCT customer_country
FROM silver.dataco_supply_chain
WHERE customer_country LIKE '%=%' OR customer_country LIKE '%ß%' OR customer_country LIKE '%¤%' 
   OR customer_country LIKE '%Ã%' OR customer_country LIKE '%~%' OR LEN(customer_country) < 3
ORDER BY customer_country;

PRINT 'Checking CUSTOMER CITY corrupted characters...';
SELECT DISTINCT customer_city
FROM silver.dataco_supply_chain
WHERE customer_city LIKE '%=%' OR customer_city LIKE '%ß%' OR customer_city LIKE '%¤%' 
   OR customer_city LIKE '%Ã%' OR customer_city LIKE '%~%' OR customer_city LIKE '%#%'
ORDER BY customer_city;

-------------------------------------------------------------------------------
--ORDER STATUS
-------------------------------------------------------------------------------
PRINT 'Checking ORDER STATUS values...';
SELECT DISTINCT order_status FROM silver.dataco_supply_chain ORDER BY order_status;

PRINT 'Flag unexpected statuses...';
SELECT DISTINCT order_status
FROM silver.dataco_supply_chain
WHERE order_status NOT IN ('Completed', 'Pending', 'Processing', 'Canceled', 'Shipped', 'Delivered');

-------------------------------------------------------------------------------
-- PRODUCT CATEGORY / DEPARTMENT NAMES
-------------------------------------------------------------------------------
PRINT 'Checking PRODUCT CATEGORY & DEPARTMENT naming quality...';
SELECT 
    SUM(CASE WHEN product_category_name IS NULL OR TRIM(product_category_name) IN ('','Unknown','N/A') THEN 1 ELSE 0 END) AS Invalid_Categories,
    SUM(CASE WHEN store_department_name IS NULL OR TRIM(store_department_name) IN ('','Unknown','N/A') THEN 1 ELSE 0 END) AS Invalid_Departments
FROM silver.dataco_supply_chain;

PRINT 'Checking strange symbols or encoding issues in product names...';
SELECT DISTINCT product_name
FROM silver.dataco_supply_chain
WHERE product_name LIKE '%=%' OR product_name LIKE '%ß%' OR product_name LIKE '%¤%' OR product_name LIKE '%Ã%' 
   OR product_name LIKE '%~%' OR product_name LIKE '%#%'
ORDER BY product_name;

-------------------------------------------------------------------------------
-- NULL / BLANK TEXT FIELDS
-------------------------------------------------------------------------------
PRINT 'Checking for NULL or blank categorical values...';
SELECT 
    SUM(CASE WHEN TRIM(order_delivery_status) = '' OR order_delivery_status IS NULL THEN 1 ELSE 0 END) AS Null_DeliveryStatus,
    SUM(CASE WHEN TRIM(shipping_mode) = '' OR shipping_mode IS NULL THEN 1 ELSE 0 END) AS Null_ShippingMode,
    SUM(CASE WHEN TRIM(types_of_customers) = '' OR types_of_customers IS NULL THEN 1 ELSE 0 END) AS Null_CustomerType,
    SUM(CASE WHEN TRIM(order_continent) = '' OR order_continent IS NULL THEN 1 ELSE 0 END) AS Null_Market,
    SUM(CASE WHEN TRIM(order_country) = '' OR order_country IS NULL THEN 1 ELSE 0 END) AS Null_OrderCountry,
    SUM(CASE WHEN TRIM(order_city) = '' OR order_city IS NULL THEN 1 ELSE 0 END) AS Null_OrderCity,
    SUM(CASE WHEN TRIM(product_category_name) = '' OR product_category_name IS NULL THEN 1 ELSE 0 END) AS Null_Category
FROM silver.dataco_supply_chain;

PRINT '=============================================================';
PRINT 'FULL CATEGORICAL VALIDATION completed for Silver layer';
PRINT 'Review above outputs for encoding errors, typos, and normalization targets.';
PRINT '=============================================================';
GO
