/*=====================================================================
 BRONZE DATA QUALITY CHECK (COLUMN-BY-COLUMN – FULL VERSION)
 Dataset : bronze.dataco_supply_chain
 Purpose : Explore & validate each of the 53 raw columns before Silver
=====================================================================*/


/***************************************************************
 0. High-level sanity: row counts & basic keys
***************************************************************/
PRINT '=== BASIC STRUCTURE CHECK ===';

SELECT 
    COUNT(*)                              AS total_rows,
    COUNT(DISTINCT [Order Id])            AS distinct_orders,
    COUNT(DISTINCT [Order Item Id])       AS distinct_order_items,
    COUNT(DISTINCT [Customer Id])         AS distinct_customers,
    COUNT(DISTINCT [Product Card Id])     AS distinct_products
FROM bronze.dataco_supply_chain;



/***************************************************************
 1. Type
    • Meaning:
         Raw indicator of pay method / transaction type.

    • DQA / QC:
         - Distinct values to detect typos and inconsistent labels.
         - Null count.
         - Suspicious characters (digits / symbols).
***************************************************************/
PRINT '--- 1. Type ---';

-- Distinct raw values
SELECT DISTINCT Type
FROM bronze.dataco_supply_chain;

-- Nulls
SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE Type IS NULL;

-- Suspicious characters (non letters / space)
SELECT DISTINCT Type AS suspicious_type
FROM bronze.dataco_supply_chain
WHERE Type LIKE '%[^A-Za-z ]%' AND Type IS NOT NULL;



/***************************************************************
 2. Days for shipping (real)
    • Meaning:
         Observed days between order and delivery.

    • DQA / QC:
         - Min / max / nulls.
         - Negative values.
         - Very large values (e.g., > 60 days).
***************************************************************/
PRINT '--- 2. Days for shipping (real) ---';

-- Summary stats
SELECT 
    MIN([Days for shipping (real)]) AS min_value,
    MAX([Days for shipping (real)]) AS max_value,
    COUNT(*)                        AS total_rows,
    SUM(CASE WHEN [Days for shipping (real)] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

-- Suspicious values
SELECT *
FROM bronze.dataco_supply_chain
WHERE [Days for shipping (real)] < 0
   OR [Days for shipping (real)] > 60;



/***************************************************************
 3. Days for shipment (scheduled)
    • Meaning:
         Planned / promised delivery duration in days.

    • DQA / QC:
         - Min / max / nulls.
         - Negative or very large values.
         - Compare with real days where possible.
***************************************************************/
PRINT '--- 3. Days for shipment (scheduled) ---';

-- Summary stats
SELECT 
    MIN([Days for shipment (scheduled)]) AS min_value,
    MAX([Days for shipment (scheduled)]) AS max_value,
    COUNT(*)                             AS total_rows,
    SUM(CASE WHEN [Days for shipment (scheduled)] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

-- Suspicious values
SELECT *
FROM bronze.dataco_supply_chain
WHERE [Days for shipment (scheduled)] < 0
   OR [Days for shipment (scheduled)] > 60;



/***************************************************************
 4. Benefit per order
    • Meaning:
         Monetary benefit / earnings allocated to this line's order.

    • DQA / QC:
         - Min / max / nulls.
         - Negative values.
         - Very large outliers.
***************************************************************/
PRINT '--- 4. Benefit per order ---';

SELECT 
    MIN([Benefit per order]) AS min_value,
    MAX([Benefit per order]) AS max_value,
    COUNT(*)                 AS total_rows,
    SUM(CASE WHEN [Benefit per order] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Benefit per order] < 0
   OR [Benefit per order] > 10000;  -- arbitrary high outlier check, adjust if needed



/***************************************************************
 5. Sales per customer
    • Meaning:
         Cumulative sales amount per customer.

    • DQA / QC:
         - Min / max / nulls.
         - Negative values.
         - Very large outliers.
***************************************************************/
PRINT '--- 5. Sales per customer ---';

SELECT 
    MIN([Sales per customer]) AS min_value,
    MAX([Sales per customer]) AS max_value,
    COUNT(*)                  AS total_rows,
    SUM(CASE WHEN [Sales per customer] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Sales per customer] < 0
   OR [Sales per customer] > 100000;  -- sanity upper bound



/***************************************************************
 6. Delivery Status
    • Meaning:
         Text label describing outcome of delivery (e.g., "Late", "On time").

    • DQA / QC:
         - Distinct values (to find inconsistent labels).
         - Nulls.
***************************************************************/
PRINT '--- 6. Delivery Status ---';

SELECT DISTINCT [Delivery Status]
FROM bronze.dataco_supply_chain;

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Delivery Status] IS NULL;



/***************************************************************
 7. Late_delivery_risk
    • Meaning:
         Flag or score for whether delivery is late.

    • DQA / QC:
         - Distinct values (expect mostly 0/1).
         - Check rows where flag = 1 but real days <= scheduled days.
***************************************************************/
PRINT '--- 7. Late_delivery_risk ---';

-- Distinct raw values
SELECT DISTINCT [Late_delivery_risk]
FROM bronze.dataco_supply_chain;

-- Inconsistent rows: flagged late but not actually later than scheduled
SELECT *
FROM bronze.dataco_supply_chain
WHERE [Late_delivery_risk] = 1
  AND [Days for shipping (real)] IS NOT NULL
  AND [Days for shipment (scheduled)] IS NOT NULL
  AND [Days for shipping (real)] <= [Days for shipment (scheduled)];



/***************************************************************
 8. Category Id
    • Meaning:
         Numeric identifier for product category.

    • DQA / QC:
         - Null count.
         - Number of distinct categories.
***************************************************************/
PRINT '--- 8. Category Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Category Id] IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT [Category Id]) AS distinct_categories
FROM bronze.dataco_supply_chain;



/***************************************************************
 9. Category Name
    • Meaning:
         Text description of product category.

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 9. Category Name ---';

SELECT DISTINCT [Category Name]
FROM bronze.dataco_supply_chain
ORDER BY [Category Name];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Category Name] IS NULL;



/***************************************************************
 10. Customer City
    • Meaning:
         City where the customer resides.

    • DQA / QC:
         - Distinct cities.
         - Null count.
***************************************************************/
PRINT '--- 10. Customer City ---';

SELECT DISTINCT [Customer City]
FROM bronze.dataco_supply_chain
ORDER BY [Customer City];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer City] IS NULL;



/***************************************************************
 11. Customer Country
    • Meaning:
         Country where the customer resides.

    • DQA / QC:
         - Distinct raw values (identify weird spellings like "MTxico").
         - Nulls.
***************************************************************/
PRINT '--- 11. Customer Country ---';

SELECT DISTINCT [Customer Country]
FROM bronze.dataco_supply_chain
ORDER BY [Customer Country];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Country] IS NULL;



/***************************************************************
 12. Customer Email
    • Meaning:
         Customer email address (PII, not used in analytics).

    • DQA / QC:
         - Nulls.
         - Basic pattern check (must contain '@' and '.').
***************************************************************/
PRINT '--- 12. Customer Email ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Email] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Customer Email] IS NOT NULL
  AND [Customer Email] NOT LIKE '%@%.%';



/***************************************************************
 13. Customer Fname
    • Meaning:
         Customer first name.

    • DQA / QC:
         - Nulls.
         - Suspiciously short values.
***************************************************************/
PRINT '--- 13. Customer Fname ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Fname] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Customer Fname] IS NOT NULL
  AND LEN([Customer Fname]) < 2;



/***************************************************************
 14. Customer Lname
    • Meaning:
         Customer last name.

    • DQA / QC:
         - Nulls.
         - Suspiciously short values.
***************************************************************/
PRINT '--- 14. Customer Lname ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Lname] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Customer Lname] IS NOT NULL
  AND LEN([Customer Lname]) < 2;



/***************************************************************
 15. Customer Id
    • Meaning:
         Unique identifier for customer.

    • DQA / QC:
         - Nulls.
         - Count distinct vs total.
***************************************************************/
PRINT '--- 15. Customer Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Customer Id] IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT [Customer Id]) AS distinct_customers
FROM bronze.dataco_supply_chain;



/***************************************************************
 16. Customer Password
    • Meaning:
         Customer password (PII, not used later).

    • DQA / QC:
         - Null count.
***************************************************************/
PRINT '--- 16. Customer Password ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Password] IS NULL;



/***************************************************************
 17. Customer Segment
    • Meaning:
         Segment classification (e.g., Consumer, Corporate).

    • DQA / QC:
         - Distinct values.
         - Null count.
***************************************************************/
PRINT '--- 17. Customer Segment ---';

SELECT DISTINCT [Customer Segment]
FROM bronze.dataco_supply_chain
ORDER BY [Customer Segment];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Segment] IS NULL;



/***************************************************************
 18. Customer State
    • Meaning:
         State / region of customer.

    • DQA / QC:
         - Distinct values for mapping.
         - Null count.
***************************************************************/
PRINT '--- 18. Customer State ---';

SELECT DISTINCT [Customer State]
FROM bronze.dataco_supply_chain
ORDER BY [Customer State];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer State] IS NULL;



/***************************************************************
 19. Customer Street
    • Meaning:
         Full street line (number + name).

    • DQA / QC:
         - Nulls.
         - Values without space (hard to split).
***************************************************************/
PRINT '--- 19. Customer Street ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Customer Street] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Customer Street] IS NOT NULL
  AND [Customer Street] NOT LIKE '% %';



/***************************************************************
 20. Customer Zipcode
    • Meaning:
         Postal code for customer location.

    • DQA / QC:
         - Nulls.
         - Non-positive values.
***************************************************************/
PRINT '--- 20. Customer Zipcode ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Customer Zipcode] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Customer Zipcode] IS NOT NULL
  AND [Customer Zipcode] <= 0;



/***************************************************************
 21. Department Id
    • Meaning:
         Identifier of department / store section.

    • DQA / QC:
         - Nulls.
         - Distinct count.
***************************************************************/
PRINT '--- 21. Department Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Department Id] IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT [Department Id]) AS distinct_departments
FROM bronze.dataco_supply_chain;



/***************************************************************
 22. Department Name
    • Meaning:
         Name of department / store section.

    • DQA / QC:
         - Nulls.
         - Distinct values.
***************************************************************/
PRINT '--- 22. Department Name ---';

SELECT DISTINCT [Department Name]
FROM bronze.dataco_supply_chain
ORDER BY [Department Name];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Department Name] IS NULL;



/***************************************************************
 23. Latitude
    • Meaning:
         Store location latitude.

    • DQA / QC:
         - Values must be between -90 and 90.
         - Nulls.
***************************************************************/
PRINT '--- 23. Latitude ---';

SELECT 
    MIN([Latitude]) AS min_value,
    MAX([Latitude]) AS max_value,
    SUM(CASE WHEN [Latitude] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Latitude] IS NOT NULL
  AND ([Latitude] < -90 OR [Latitude] > 90);



/***************************************************************
 24. Longitude
    • Meaning:
         Store location longitude.

    • DQA / QC:
         - Values must be between -180 and 180.
         - Nulls.
***************************************************************/
PRINT '--- 24. Longitude ---';

SELECT 
    MIN([Longitude]) AS min_value,
    MAX([Longitude]) AS max_value,
    SUM(CASE WHEN [Longitude] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Longitude] IS NOT NULL
  AND ([Longitude] < -180 OR [Longitude] > 180);



/***************************************************************
 25. Market
    • Meaning:
         Market code (e.g., USCA, LATAM).

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 25. Market ---';

SELECT DISTINCT Market
FROM bronze.dataco_supply_chain
ORDER BY Market;

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE Market IS NULL;



/***************************************************************
 26. Order City
    • Meaning:
         City where the order is delivered.

    • DQA / QC:
         - Distinct values.
         - Null count.
***************************************************************/
PRINT '--- 26. Order City ---';

SELECT DISTINCT [Order City]
FROM bronze.dataco_supply_chain
ORDER BY [Order City];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Order City] IS NULL;



/***************************************************************
 27. Order Country
    • Meaning:
         Country of order destination.

    • DQA / QC:
         - Distinct values.
         - Null count.
***************************************************************/
PRINT '--- 27. Order Country ---';

SELECT DISTINCT [Order Country]
FROM bronze.dataco_supply_chain
ORDER BY [Order Country];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Order Country] IS NULL;



/***************************************************************
 28. Order Customer Id
    • Meaning:
         Customer Id associated with the order.

    • DQA / QC:
         - Nulls.
         - Distinct vs Customer Id consistency (optional).
***************************************************************/
PRINT '--- 28. Order Customer Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Order Customer Id] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;



/***************************************************************
 29. order date (DateOrders)
    • Meaning:
         Date/time when order was placed.

    • DQA / QC:
         - Nulls.
         - Future dates.
***************************************************************/
PRINT '--- 29. order date (DateOrders) ---';

SELECT 
    MIN([order date (DateOrders)]) AS min_date,
    MAX([order date (DateOrders)]) AS max_date,
    SUM(CASE WHEN [order date (DateOrders)] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [order date (DateOrders)] > GETDATE();



/***************************************************************
 30. Order Id
    • Meaning:
         Identifier of the order.

    • DQA / QC:
         - Nulls.
         - Distinct count.
***************************************************************/
PRINT '--- 30. Order Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Order Id] IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT [Order Id]) AS distinct_orders
FROM bronze.dataco_supply_chain;



/***************************************************************
 31. Order Item Cardprod Id
    • Meaning:
         Internal link from line item to product card.

    • DQA / QC:
         - Nulls.
***************************************************************/
PRINT '--- 31. Order Item Cardprod Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Order Item Cardprod Id] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;



/***************************************************************
 32. Order Item Discount
    • Meaning:
         Absolute discount value for this line item.

    • DQA / QC:
         - Negative values.
         - Huge discounts vs Sales (optional).
***************************************************************/
PRINT '--- 32. Order Item Discount ---';

SELECT 
    MIN([Order Item Discount]) AS min_value,
    MAX([Order Item Discount]) AS max_value,
    SUM(CASE WHEN [Order Item Discount] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Discount] < 0;



/***************************************************************
 33. Order Item Discount Rate
    • Meaning:
         Discount rate (ratio).

    • DQA / QC:
         - Values below 0 or above 1 suspicious.
***************************************************************/
PRINT '--- 33. Order Item Discount Rate ---';

SELECT 
    MIN([Order Item Discount Rate]) AS min_value,
    MAX([Order Item Discount Rate]) AS max_value,
    SUM(CASE WHEN [Order Item Discount Rate] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Discount Rate] < 0
   OR [Order Item Discount Rate] > 1.5;  -- allow slightly over 1 for data issues



/***************************************************************
 34. Order Item Id
    • Meaning:
         Unique identifier of the order line.

    • DQA / QC:
         - Nulls.
         - Duplicate Ids.
***************************************************************/
PRINT '--- 34. Order Item Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Order Item Id] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT [Order Item Id], COUNT(*) AS duplicate_count
FROM bronze.dataco_supply_chain
GROUP BY [Order Item Id]
HAVING COUNT(*) > 1;



/***************************************************************
 35. Order Item Product Price
    • Meaning:
         Price per product on this line.

    • DQA / QC:
         - Negative values.
         - Outliers.
***************************************************************/
PRINT '--- 35. Order Item Product Price ---';

SELECT 
    MIN([Order Item Product Price]) AS min_value,
    MAX([Order Item Product Price]) AS max_value,
    SUM(CASE WHEN [Order Item Product Price] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Product Price] < 0;



/***************************************************************
 36. Order Item Profit Ratio
    • Meaning:
         Profit ratio metric at line level.

    • DQA / QC:
         - Negative values.
         - Extremely large values.
***************************************************************/
PRINT '--- 36. Order Item Profit Ratio ---';

SELECT 
    MIN([Order Item Profit Ratio]) AS min_value,
    MAX([Order Item Profit Ratio]) AS max_value,
    SUM(CASE WHEN [Order Item Profit Ratio] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Profit Ratio] < -1
   OR [Order Item Profit Ratio] > 10;



/***************************************************************
 37. Order Item Quantity
    • Meaning:
         Quantity of units purchased on this line.

    • DQA / QC:
         - Zero or negative quantities.
***************************************************************/
PRINT '--- 37. Order Item Quantity ---';

SELECT 
    MIN([Order Item Quantity]) AS min_value,
    MAX([Order Item Quantity]) AS max_value,
    SUM(CASE WHEN [Order Item Quantity] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Quantity] <= 0;



/***************************************************************
 38. Sales
    • Meaning:
         Sales value for this line (gross).

    • DQA / QC:
         - Negative sales.
         - Extreme outliers.
***************************************************************/
PRINT '--- 38. Sales ---';

SELECT 
    MIN(Sales) AS min_value,
    MAX(Sales) AS max_value,
    SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE Sales < 0
   OR Sales > 10000;



/***************************************************************
 39. Order Item Total
    • Meaning:
         Net total for this line (after discount).

    • DQA / QC:
         - Negative totals.
         - Net > Sales.
***************************************************************/
PRINT '--- 39. Order Item Total ---';

SELECT 
    MIN([Order Item Total]) AS min_value,
    MAX([Order Item Total]) AS max_value,
    SUM(CASE WHEN [Order Item Total] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Item Total] < 0
   OR ([Order Item Total] > Sales AND Sales IS NOT NULL);



/***************************************************************
 40. Order Profit Per Order
    • Meaning:
         Profit metric assigned to this order.

    • DQA / QC:
         - Negative profits.
         - Very large outliers.
***************************************************************/
PRINT '--- 40. Order Profit Per Order ---';

SELECT 
    MIN([Order Profit Per Order]) AS min_value,
    MAX([Order Profit Per Order]) AS max_value,
    SUM(CASE WHEN [Order Profit Per Order] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Profit Per Order] < -1000
   OR [Order Profit Per Order] > 10000;



/***************************************************************
 41. Order Region
    • Meaning:
         Region grouping for the order destination.

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 41. Order Region ---';

SELECT DISTINCT [Order Region]
FROM bronze.dataco_supply_chain
ORDER BY [Order Region];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Order Region] IS NULL;



/***************************************************************
 42. Order State
    • Meaning:
         State of order destination.

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 42. Order State ---';

SELECT DISTINCT [Order State]
FROM bronze.dataco_supply_chain
ORDER BY [Order State];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Order State] IS NULL;



/***************************************************************
 43. Order Status
    • Meaning:
         Status of the order (e.g., COMPLETE, CANCELED).

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 43. Order Status ---';

SELECT DISTINCT [Order Status]
FROM bronze.dataco_supply_chain
ORDER BY [Order Status];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Order Status] IS NULL;



/***************************************************************
 44. Order Zipcode
    • Meaning:
         Postal code of order destination.

    • DQA / QC:
         - Nulls.
         - Non-positive values.
***************************************************************/
PRINT '--- 44. Order Zipcode ---';

SELECT 
    MIN([Order Zipcode]) AS min_value,
    MAX([Order Zipcode]) AS max_value,
    SUM(CASE WHEN [Order Zipcode] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Order Zipcode] IS NOT NULL
  AND [Order Zipcode] <= 0;



/***************************************************************
 45. Product Card Id
    • Meaning:
         Identifier for the product card.

    • DQA / QC:
         - Nulls.
         - Lines per product.
***************************************************************/
PRINT '--- 45. Product Card Id ---';

SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN [Product Card Id] IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT [Product Card Id]) AS distinct_products
FROM bronze.dataco_supply_chain;



/***************************************************************
 46. Product Category Id
    • Meaning:
         Alternate / secondary product category id.

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 46. Product Category Id ---';

SELECT DISTINCT [Product Category Id]
FROM bronze.dataco_supply_chain
ORDER BY [Product Category Id];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Product Category Id] IS NULL;



/***************************************************************
 47. Product Description
    • Meaning:
         Text description of the product.

    • DQA / QC:
         - Null rate.
         - Very short descriptions.
***************************************************************/
PRINT '--- 47. Product Description ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Product Description] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Product Description] IS NOT NULL
  AND LEN([Product Description]) < 5;



/***************************************************************
 48. Product Image
    • Meaning:
         Reference to product image asset.

    • DQA / QC:
         - Null rate.
***************************************************************/
PRINT '--- 48. Product Image ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Product Image] IS NULL;



/***************************************************************
 49. Product Name
    • Meaning:
         The product's display name.

    • DQA / QC:
         - Very short names.
         - Nulls.
***************************************************************/
PRINT '--- 49. Product Name ---';

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Product Name] IS NULL;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Product Name] IS NOT NULL
  AND LEN([Product Name]) < 3;



/***************************************************************
 50. Product Price
    • Meaning:
         List price of product.

    • DQA / QC:
         - Negative prices.
         - Very large outliers.
***************************************************************/
PRINT '--- 50. Product Price ---';

SELECT 
    MIN([Product Price]) AS min_value,
    MAX([Product Price]) AS max_value,
    SUM(CASE WHEN [Product Price] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [Product Price] < 0
   OR [Product Price] > 10000;



/***************************************************************
 51. Product Status
    • Meaning:
         Status flag for product (e.g., active/inactive).

    • DQA / QC:
         - Distinct values.
         - Nulls.
***************************************************************/
PRINT '--- 51. Product Status ---';

SELECT DISTINCT [Product Status]
FROM bronze.dataco_supply_chain
ORDER BY [Product Status];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Product Status] IS NULL;



/***************************************************************
 52. shipping date (DateOrders)
    • Meaning:
         Date/time when shipment was sent / processed.

    • DQA / QC:
         - Nulls.
         - Future dates.
         - Shipping before order date.
***************************************************************/
PRINT '--- 52. shipping date (DateOrders) ---';

SELECT 
    MIN([shipping date (DateOrders)]) AS min_ship_date,
    MAX([shipping date (DateOrders)]) AS max_ship_date,
    SUM(CASE WHEN [shipping date (DateOrders)] IS NULL THEN 1 ELSE 0 END) AS null_count
FROM bronze.dataco_supply_chain;

SELECT *
FROM bronze.dataco_supply_chain
WHERE [shipping date (DateOrders)] > GETDATE()
   OR ([order date (DateOrders)] IS NOT NULL 
       AND [shipping date (DateOrders)] IS NOT NULL
       AND [shipping date (DateOrders)] < [order date (DateOrders)]);



/***************************************************************
 53. Shipping Mode
    • Meaning:
         Shipping option chosen (e.g., Standard Class, First Class).

    • DQA / QC:
         - Distinct values to see all modes.
         - Nulls.
***************************************************************/
PRINT '--- 53. Shipping Mode ---';

SELECT DISTINCT [Shipping Mode]
FROM bronze.dataco_supply_chain
ORDER BY [Shipping Mode];

SELECT COUNT(*) AS null_count
FROM bronze.dataco_supply_chain
WHERE [Shipping Mode] IS NULL;



PRINT '==================== END OF BRONZE DQA =====================';
