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
    PRINT 'Starting Silver Layer Load';
    PRINT '=========================================================';

    -- Clear existing Silver data
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
        customer_street_number,
        customer_street_name,
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
        order_item_product_barcode_id,
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
        shipping_mode,
        shipping_date,
        shipping_time
    )
    SELECT
        /* PAYMENT TYPE NORMALIZATION */
        CASE 
            WHEN Type = 'CASH' THEN 'Cash'
            WHEN Type = 'DEBIT' THEN 'Debit'
            WHEN Type = 'TRANSFER' THEN 'Transfer (unspecified)'
            WHEN Type = 'PAYMENT' THEN 'Payment (unspecified)'
            ELSE 'Unknown'
        END AS payment_type,

        /* SHIPPING DAYS */
        [Days for shipping (real)],
        [Days for shipment (scheduled)],

        /* MONEY FIELDS */
        [Benefit per order],
        [Sales per customer],

        /* DELIVERY FIELDS */
        [Delivery Status],
        [Late_delivery_risk],

        /* PRODUCT CATEGORY */
        [Category Id],
        [Category Name],

        /* CUSTOMER LOCATION */
        [Customer City],

        CASE 
            WHEN [Customer Country] IN ('EE. UU.', 'US', 'U.S.') THEN 'United States'
            WHEN [Customer Country] = 'MTxico' THEN 'México'
            WHEN [Customer Country] = 'Panamß' THEN 'Panamá'
            ELSE [Customer Country]
        END AS customer_country,

        /* CUSTOMER FULL NAME */
        CONCAT(COALESCE([Customer Fname], ''), ' ', COALESCE([Customer Lname], '')),

        [Customer Id],
        [Customer Segment],

        /* CUSTOMER STATE NORMALIZATION */
        CASE 
            WHEN [Customer State] = 'UT' THEN 'Utah'
            WHEN [Customer State] = 'WI' THEN 'Wisconsin'
            WHEN [Customer State] = 'NC' THEN 'North Carolina'
            WHEN [Customer State] = 'MI' THEN 'Michigan'
            WHEN [Customer State] = 'TN' THEN 'Tennessee'
            WHEN [Customer State] = 'OK' THEN 'Oklahoma'
            WHEN [Customer State] = 'KY' THEN 'Kentucky'
            WHEN [Customer State] = 'CO' THEN 'Colorado'
            WHEN [Customer State] = 'NV' THEN 'Nevada'
            WHEN [Customer State] = 'PA' THEN 'Pennsylvania'
            WHEN [Customer State] = 'WV' THEN 'West Virginia'
            WHEN [Customer State] = 'GA' THEN 'Georgia'
            WHEN [Customer State] = 'RI' THEN 'Rhode Island'
            WHEN [Customer State] = 'IN' THEN 'Indiana'
            WHEN [Customer State] = 'DC' THEN 'District of Columbia'
            WHEN [Customer State] = 'MD' THEN 'Maryland'
            WHEN [Customer State] = 'OR' THEN 'Oregon'
            WHEN [Customer State] = 'CT' THEN 'Connecticut'
            WHEN [Customer State] = 'AR' THEN 'Arkansas'
            WHEN [Customer State] = 'AL' THEN 'Alabama'
            WHEN [Customer State] = 'MN' THEN 'Minnesota'
            WHEN [Customer State] = 'ID' THEN 'Idaho'
            WHEN [Customer State] = 'TX' THEN 'Texas'
            WHEN [Customer State] = 'NM' THEN 'New Mexico'
            WHEN [Customer State] = 'ND' THEN 'North Dakota'
            WHEN [Customer State] = 'PR' THEN 'Puerto Rico'
            WHEN [Customer State] = 'IL' THEN 'Illinois'
            WHEN [Customer State] = 'MO' THEN 'Missouri'
            WHEN [Customer State] = 'SC' THEN 'South Carolina'
            WHEN [Customer State] = 'DE' THEN 'Delaware'
            WHEN [Customer State] = 'FL' THEN 'Florida'
            WHEN [Customer State] = 'CA' THEN 'California'
            WHEN [Customer State] = 'HI' THEN 'Hawaii'
            WHEN [Customer State] = 'OH' THEN 'Ohio'
            WHEN [Customer State] = 'NY' THEN 'New York'
            WHEN [Customer State] = 'NJ' THEN 'New Jersey'
            WHEN [Customer State] = 'IA' THEN 'Iowa'
            WHEN [Customer State] = 'KS' THEN 'Kansas'
            WHEN [Customer State] = 'LA' THEN 'Louisiana'
            WHEN [Customer State] = 'WA' THEN 'Washington'
            WHEN [Customer State] = 'MT' THEN 'Montana'
            WHEN [Customer State] = 'VA' THEN 'Virginia'
            WHEN [Customer State] = 'MA' THEN 'Massachusetts'
            WHEN [Customer State] = 'AZ' THEN 'Arizona'
            ELSE [Customer State]
        END AS customer_state,

        /* ADDRESS SPLIT */
        [Customer Street] AS customer_full_street,
        LEFT([Customer Street], NULLIF(CHARINDEX(' ', [Customer Street]), 0) - 1) AS customer_street_number,
        SUBSTRING([Customer Street], NULLIF(CHARINDEX(' ', [Customer Street]), 0) + 1, LEN([Customer Street])) AS customer_street_name,

        /* STORE INFO */
        [Department Id],
        [Department Name],
        [Latitude],
        [Longitude],

        /* MARKET / CONTINENT */
        CASE 
            WHEN Market = 'LATAM' THEN 'Latin America'
            WHEN Market = 'USCA' THEN 'North America'
            ELSE Market
        END AS order_continent,

        [Order City],
        [Order Country],
        [Order Customer Id],

        /* ORDER DATE SAFE CAST */
        TRY_CAST([order date (DateOrders)] AS DATE) AS order_date,

        /* ORDER TIME SAFE (HH:MM) */
        CASE 
            WHEN TRY_CAST([order date (DateOrders)] AS DATETIME) IS NOT NULL THEN 
                CONVERT(VARCHAR(5), TRY_CAST([order date (DateOrders)] AS DATETIME), 108)
        END AS order_time,

        /* ORDER KEYS */
        [Order Id],
        [Order Item Cardprod Id],

        /* DISCOUNT */
        CAST(ROUND([Order Item Discount], 1) AS FLOAT),
        CAST(ROUND([Order Item Discount Rate], 2) AS FLOAT),

        [Order Item Id],

        /* PRICE / ITEM METRICS */
        ROUND([Product Price], 1),
        [Order Item Profit Ratio],
        [Order Item Quantity],
        ROUND([Sales], 1),
        ROUND([Order Item Total], 1),
        ROUND([Order Profit Per Order], 1),

        [Order State],

        /* ORDER STATUS NORMALIZED */
        CASE 
            WHEN [Order Status] IS NULL THEN NULL
            ELSE CONCAT(
                    UPPER(LEFT(REPLACE(LOWER([Order Status]), '_', ' '), 1)), 
                    SUBSTRING(REPLACE(LOWER([Order Status]), '_', ' '), 2, LEN([Order Status]))
                )
        END AS order_status,

        [Order Region],

        [Product Card Id],
        [Product Name],

        [Shipping Mode],

        /* SHIPPING DATE SAFE CAST */
        TRY_CAST([shipping date (DateOrders)] AS DATE) AS shipping_date,

        /* SHIPPING TIME SAFE */
        CASE 
            WHEN TRY_CAST([shipping date (DateOrders)] AS DATETIME) IS NOT NULL THEN 
                CONVERT(VARCHAR(5), TRY_CAST([shipping date (DateOrders)] AS DATETIME), 108)
        END AS shipping_time

    FROM bronze.dataco_supply_chain;

    PRINT 'Silver Layer Load Completed Successfully!';
END;
GO
essfully!';
END;
GO

