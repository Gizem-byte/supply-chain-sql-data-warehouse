/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Stored Procedure : proc_load_silver
Purpose          : Transform/clean Bronze data into Silver layer
===============================================================================
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC proc.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE proc_load_silver AS
BEGIN
    SET NOCOUNT ON;

    PRINT '=========================================================';
    PRINT 'Starting Silver Layer Load (Safe Date Conversion)';
    PRINT '=========================================================';

    TRUNCATE TABLE silver.dataco_supply_chain;

    INSERT INTO silver.dataco_supply_chain (
        payment_type,
        actual_shipping_days,
        scheduled_shipping_days,
        earning_per_order_item,
        total_sale_per_customer,
        order_delivery_status,
        order_late_delivery_risk,
        product_category_id,
        product_category_name,
        customer_city,
        customer_country,
        customer_full_name,
        customer_id,
        types_of_customers,
        customer_state,
        customer_full_street,
        store_department_id,
        store_department_name,
        store_location_latitude,
        store_location_longitude,
        order_continent,
        order_city,
        order_country,
        order_customer_id,
        order_date,
        order_time,
        order_id,
        order_item_discount,
        order_item_discount_percentage,
        order_item_id,
        product_unit_price,
        order_item_profit_ratio,
        order_item_quantity,
        order_item_gross_total,
        order_item_net_total,
        order_profit_per_order_item,
        order_state,
        order_status,
        order_subregion,
        product_barcode_id,
        product_name,
        shipping_date,
        shipping_time,
        shipping_mode,
        customer_street_number,
        customer_street_name
    )
    SELECT
        /* Payment Type Normalization */
        CASE 
            WHEN Type = 'CASH' THEN 'Cash'
            WHEN Type = 'DEBIT' THEN 'Debit'
            WHEN Type = 'TRANSFER' THEN 'Transfer (unspecified)'
            WHEN Type = 'PAYMENT' THEN 'Payment (unspecified)'
            ELSE 'Unknown'
        END,

        [Days for shipping (real)],
        [Days for shipment (scheduled)],
        [Benefit per order],
        [Sales per customer],
        [Delivery Status],
        [Late_delivery_risk],
        [Category Id],
        [Category Name],
        [Customer City],

        /* Country Normalization */
        CASE 
            WHEN [Customer Country] IN ('EE. UU.', 'US', 'U.S.') THEN 'United States'
            WHEN [Customer Country] = 'MTxico' THEN 'México'
            WHEN [Customer Country] = 'Panamß' THEN 'Panamá'
            ELSE [Customer Country]
        END,

        CONCAT(COALESCE([Customer Fname], ''), ' ', COALESCE([Customer Lname], '')),
        [Customer Id],
        [Customer Segment],

        [Customer State],

        [Customer Street],
        [Department Id],
        [Department Name],
        [Latitude],
        [Longitude],

        CASE WHEN Market = 'LATAM' THEN 'Latin America'
             WHEN Market = 'USCA' THEN 'North America'
             ELSE Market END,

        [Order City],
        [Order Country],
        [Order Customer Id],

        /* ⭐ SAFE ORDER DATE PARSING ⭐ */
        CASE 
            WHEN ISDATE([order date (DateOrders)]) = 1 
            THEN CAST([order date (DateOrders)] AS DATE)
            ELSE NULL
        END AS order_date,

        CASE 
            WHEN ISDATE([order date (DateOrders)]) = 1 
            THEN FORMAT(CAST([order date (DateOrders)] AS DATETIME), 'HH:mm')
            ELSE NULL
        END AS order_time,

        [Order Id],

        ROUND([Order Item Discount], 1),
        ROUND([Order Item Discount Rate], 2),

        [Order Item Id],
        ROUND([Product Price], 1),
        [Order Item Profit Ratio],
        [Order Item Quantity],
        ROUND([Sales], 1),
        ROUND([Order Item Total], 1),
        ROUND([Order Profit Per Order], 1),

        [Order State],

        CASE WHEN [Order Status] IS NULL THEN NULL
             ELSE CONCAT(
                    UPPER(LEFT(REPLACE(LOWER([Order Status]), '_', ' '), 1)),
                    SUBSTRING(REPLACE(LOWER([Order Status]), '_', ' '), 2, LEN([Order Status]))
             )
        END,

        [Order Region],

        [Product Card Id],
        [Product Name],

        /* ⭐ SAFE SHIPPING DATE PARSING ⭐ */
        CASE 
            WHEN ISDATE([shipping date (DateOrders)]) = 1 
            THEN CAST([shipping date (DateOrders)] AS DATE)
            ELSE NULL
        END AS shipping_date,

        CASE 
            WHEN ISDATE([shipping date (DateOrders)]) = 1 
            THEN FORMAT(CAST([shipping date (DateOrders)] AS DATETIME), 'HH:mm')
            ELSE NULL
        END AS shipping_time,

        [Shipping Mode],

        /* Customer Street split safely */
        CASE WHEN CHARINDEX(' ', [Customer Street]) > 0
             THEN LEFT([Customer Street], CHARINDEX(' ', [Customer Street]) - 1)
             ELSE NULL END,

        CASE WHEN CHARINDEX(' ', [Customer Street]) > 0
             THEN SUBSTRING([Customer Street], CHARINDEX(' ', [Customer Street]) + 1, LEN([Customer Street]))
             ELSE NULL END
    FROM bronze.dataco_supply_chain;

    PRINT 'Silver Load Completed Successfully (Safe Mode)!';
END;
GO
