/*
===============================================================================
Script Name  : sense_check_bronze.sql
Purpose      : Perform sanity and completeness checks on Bronze layer
Description  :
    This script runs basic "sense checks" on bronze.dataco_supply_chain to 
    ensure data completeness, reasonableness, and business sanity before 
    transformation to the Silver layer.
===============================================================================
*/

USE Supply_Chain_Datawarehouse;
GO

PRINT '=============================================================';
PRINT 'Starting SENSE CHECKS on bronze.dataco_supply_chain';
PRINT '=============================================================';


-------------------------------------------------------------------------------
-- RECORD COUNT CHECK
-------------------------------------------------------------------------------
-- Expected: Record count should match the dataset size (180K for Dataco)
PRINT 'Checking total record count...';
SELECT COUNT(*) AS Total_Records FROM bronze.dataco_supply_chain;


-------------------------------------------------------------------------------
--NULL OR DUPLICATE ORDER IDs
-------------------------------------------------------------------------------
-- Expected: 
-- No NULL Order IDs 
-- High duplicate count is OK because each order can have multiple items
PRINT 'Checking for missing or duplicate Order IDs...';
SELECT 
    SUM(CASE WHEN [Order Id] IS NULL THEN 1 ELSE 0 END) AS Null_OrderId_Count,
    COUNT([Order Id]) - COUNT(DISTINCT [Order Id]) AS Duplicate_OrderId_Count
FROM bronze.dataco_supply_chain;


-------------------------------------------------------------------------------
--NEGATIVE OR UNREALISTIC PROFIT VALUES
-------------------------------------------------------------------------------
-- Expected:
-- No negative profit/benefit unless representing refunds or losses
-- A few negatives may exist, but high counts could mean data issues
PRINT 'Checking for negative or unrealistic profit metrics...';
SELECT 
    SUM(CASE WHEN [Benefit per order] < 0 THEN 1 ELSE 0 END) AS Negative_Benefit_Count,
    SUM(CASE WHEN [Order Profit Per Order] < 0 THEN 1 ELSE 0 END) AS Negative_OrderProfit_Count
FROM bronze.dataco_supply_chain;


-------------------------------------------------------------------------------
--SALES VALUE SANITY
-------------------------------------------------------------------------------
-- Expected:
-- All sales amounts should be ≥ 0 
-- Negative or zero values indicate data errors
PRINT 'Checking sales-related fields...';
SELECT 
    SUM(CASE WHEN [Sales] < 0 THEN 1 ELSE 0 END) AS Negative_Sales_Count,
    SUM(CASE WHEN [Sales per customer] < 0 THEN 1 ELSE 0 END) AS Negative_SalesPerCustomer_Count
FROM bronze.dataco_supply_chain;


-------------------------------------------------------------------------------
--SHIPPING DAYS REASONABLENESS
-------------------------------------------------------------------------------
-- Expected:
-- Shipping days ≥ 0 and usually < 60
-- Negative = data error
-- Extreme (>60) = unrealistic or outlier
PRINT 'Checking shipping day values...';
SELECT 
    SUM(CASE WHEN [Days for shipping (real)] < 0 THEN 1 ELSE 0 END) AS Negative_RealDays,
    SUM(CASE WHEN [Days for shipping (real)] > 60 THEN 1 ELSE 0 END) AS Extreme_RealDays,
    SUM(CASE WHEN [Days for shipment (scheduled)] < 0 THEN 1 ELSE 0 END) AS Negative_ScheduledDays,
    SUM(CASE WHEN [Days for shipment (scheduled)] > 60 THEN 1 ELSE 0 END) AS Extreme_ScheduledDays
FROM bronze.dataco_supply_chain;


-------------------------------------------------------------------------------
--LATE DELIVERY RISK VALIDATION
-------------------------------------------------------------------------------
-- Expected:
-- Only 0 and 1 allowed (binary flag)
PRINT 'Checking Late Delivery Risk Flag (should be 0 or 1 only)...';
SELECT DISTINCT [Late_delivery_risk]
FROM bronze.dataco_supply_chain
WHERE [Late_delivery_risk] NOT IN (0,1);


-------------------------------------------------------------------------------
--DELIVERY STATUS SANITY
-------------------------------------------------------------------------------
-- Expected:
-- Common categories: 'Delivered', 'Canceled', 'Shipping', 'Processing'
-- Unexpected values should be flagged and standardized in Silver
PRINT 'Checking unexpected Delivery Status values...';
SELECT DISTINCT [Delivery Status]
FROM bronze.dataco_supply_chain
WHERE [Delivery Status] NOT IN ('Delivered','Canceled','On Hold','Shipping','Processing','Shipped');


-------------------------------------------------------------------------------
--SHIPPING MODE SANITY
-------------------------------------------------------------------------------
-- Expected:
-- Valid modes: 'Standard Class', 'Second Class', 'First Class', 'Same Day'
-- Extra or misspelled values should be cleaned later
PRINT 'Checking Shipping Mode values...';
SELECT DISTINCT [Shipping Mode]
FROM bronze.dataco_supply_chain
WHERE [Shipping Mode] NOT IN ('Standard Class','Second Class','First Class','Same Day');


-------------------------------------------------------------------------------
--MARKET SANITY
-------------------------------------------------------------------------------
-- Expected:
-- Markets should belong to known regions: 'LATAM', 'USCA', 'Europe', 'APAC', 'MEA'
-- Unrecognized labels (like 'Pacific Asia') should be standardized
PRINT 'Checking Market values...';
SELECT DISTINCT [Market]
FROM bronze.dataco_supply_chain
WHERE [Market] NOT IN ('LATAM','USCA','Europe','Africa','APAC','MEA','Canada');


-------------------------------------------------------------------------------
-- PRODUCT PRICE REASONABLENESS
-------------------------------------------------------------------------------
-- Expected:
-- Product prices > 0 
-- Most between 1 and 10,000; extreme values should be reviewed
PRINT 'Checking product price ranges...';
SELECT 
    COUNT(*) AS OutOfRange_ProductPrice_Count
FROM bronze.dataco_supply_chain
WHERE [Product Price] <= 0 OR [Product Price] > 10000;


-------------------------------------------------------------------------------
--DATE SANITY (ORDER VS SHIPPING)
-------------------------------------------------------------------------------
-- Expected:
-- Order date ≤ Shipping date 
-- Any record where order > shipping = invalid time sequence
PRINT 'Checking temporal consistency (Order vs Shipping Dates)...';
SELECT 
    COUNT(*) AS Invalid_Date_Sequence_Count
FROM bronze.dataco_supply_chain
WHERE [order date (DateOrders)] > [shipping date (DateOrders)];


-------------------------------------------------------------------------------
--COORDINATE RANGE SANITY
-------------------------------------------------------------------------------
-- Expected:
-- Latitude between -90 and 90
-- Longitude between -180 and 180
-- Any other values = invalid coordinates
PRINT 'Checking latitude and longitude validity...';
SELECT 
    SUM(CASE WHEN [Latitude] < -90 OR [Latitude] > 90 THEN 1 ELSE 0 END) AS Invalid_Latitude_Count,
    SUM(CASE WHEN [Longitude] < -180 OR [Longitude] > 180 THEN 1 ELSE 0 END) AS Invalid_Longitude_Count
FROM bronze.dataco_supply_chain;


PRINT '=============================================================';
PRINT 'SENSE CHECKS completed for Bronze layer';
PRINT 'Review the above results and investigate any anomalies.';
PRINT '=============================================================';
GO
