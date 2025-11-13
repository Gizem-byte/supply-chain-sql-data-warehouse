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

    -- Insert transformed and cleaned data from Bronze
    INSERT INTO silver.dataco_supply_chain (
        payment_type,
        actual_shipping_days,
        scheduled_delivery_days,
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
        customer_zipcode,
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
        order_zipcode,
        order_subregion,
        product_barcode_id,
        product_name,
        shipping_mode,
        shipping_date,
        shipping_time
    )
    SELECT
        /* --- Payment Type Normalization --- */
        CASE 
            WHEN Type = 'CASH' THEN 'Cash'
            WHEN Type = 'DEBIT' THEN 'Debit'
            WHEN Type = 'TRANSFER' THEN 'Transfer (unspecified)'
            WHEN Type = 'PAYMENT' THEN 'Payment (unspecified)'
            ELSE 'Unknown'
        END AS payment_type,

        [Days for shipping (real)] AS actual_shipping_days,
        [Days for shipment (scheduled)] AS scheduled_delivery_days,
        [Benefit per order] AS earning_per_order_item,
        [Sales per customer] AS total_sale_per_customer,
        [Delivery Status] AS order_delivery_status,
        [Late_delivery_risk] AS order_late_delivery_risk,
        [Category Id] AS product_category_id,
        [Category Name] AS product_category_name,
        [Customer City] AS customer_city,

        /* --- Country Normalization --- */
        CASE 
            WHEN [Customer Country] IN ('EE. UU.', 'US', 'U.S.') THEN 'United States'
            WHEN [Customer Country] = 'MTxico' THEN 'México'
            WHEN [Customer Country] = 'Panamß' THEN 'Panamá'
            ELSE [Customer Country]
        END AS customer_country,

        /* --- Full Name Merge --- */
        CONCAT(COALESCE([Customer Fname], ''), ' ', COALESCE([Customer Lname], '')) AS customer_full_name,
        [Customer Id] AS customer_id,
        [Customer Segment] AS types_of_customers,

        /* --- Customer State Normalization --- */
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

        [Customer Street] AS customer_full_street,
        [Customer Zipcode] AS customer_zipcode,
        LEFT([Customer Street], CHARINDEX(' ', [Customer Street]) - 1) AS customer_street_number,
        RIGHT([Customer Street], LEN([Customer Street]) - CHARINDEX(' ', [Customer Street])) AS customer_street_name,

        [Department Id] AS store_department_id,
        [Department Name] AS store_department_name,
        [Latitude] AS store_location_latitude,
        [Longitude] AS store_location_longitude,

        /* --- Market Normalization --- */
        CASE 
            WHEN Market = 'LATAM' THEN 'Latin America'
            WHEN Market = 'USCA' THEN 'North America'
            ELSE Market
        END AS order_continent,

        [Order City] AS order_city,
        [Order Country] AS order_country,
        [Order Customer Id] AS order_customer_id,

        /* --- Order Date & Time Split --- */
        TRY_CAST([order date (DateOrders)] AS DATE) AS order_date,
        FORMAT(TRY_CAST([order date (DateOrders)] AS DATETIME), 'HH:mm') AS order_time,

        [Order Id] AS order_id,
        [Order Item Cardprod Id] AS order_item_product_barcode_id,

        CAST(ROUND([Order Item Discount], 1) AS FLOAT) AS order_item_discount,
        CAST(ROUND([Order Item Discount Rate], 2) AS FLOAT) AS order_item_discount_percentage,
        [Order Item Id] AS order_item_id,

        /* --- Numeric Rounding for Monetary Columns --- */
        ROUND([Product Price], 1) AS product_unit_price,
        [Order Item Profit Ratio] AS order_item_profit_ratio,
        [Order Item Quantity] AS order_item_quantity,
        ROUND([Sales], 1) AS order_item_gross_total,
        ROUND([Order Item Total], 1) AS order_item_net_total,
        ROUND([Order Profit Per Order], 1) AS order_profit_per_order_item,

        [Order State] AS order_state,

        /* --- Order Status Normalization: Title Case --- */
        CASE 
            WHEN [Order Status] IS NULL THEN NULL
            ELSE 
                CONCAT(
                    UPPER(LEFT(REPLACE(LOWER([Order Status]), '_', ' '), 1)), 
                    SUBSTRING(REPLACE(LOWER([Order Status]), '_', ' '), 2, LEN([Order Status]))
                )
        END AS order_status,

        [Order Zipcode] AS order_zipcode,
        [Order Region] AS order_subregion,
        [Product Card Id] AS product_barcode_id,
        [Product Name] AS product_name,
        [Shipping Mode] AS shipping_mode,

        /* --- Shipping Date & Time Split --- */
        TRY_CAST([shipping date (DateOrders)] AS DATE) AS shipping_date,
        FORMAT(TRY_CAST([shipping date (DateOrders)] AS DATETIME), 'HH:mm') AS shipping_time

    FROM bronze.dataco_supply_chain;

    PRINT 'Silver Layer Load Completed Successfully!';
END;
GO

