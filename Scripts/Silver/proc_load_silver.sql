/*
===============================================================================
Stored Procedure : proc_load_silver
Layer            : Bronze → Silver

Purpose:
    Loads raw data from bronze.dataco_supply_chain into the Silver layer with
    standardized, cleaned, and analytics-ready fields.

What this code does (short version):
    • Truncates the Silver table for a fresh reload.
    • Renames and standardizes column names.
    • Cleans inconsistent text values (countries, payment type, order status).
    • Splits datetime fields into separate date and time columns.
    • Rounds monetary and percentage fields for consistency.
    • Derives needed fields (full name, street number/name).
    • Preserves all analytical line-level columns needed for normalization 
      and the Gold star schema.

Notes:
    Silver keeps **one row per order item** and is the clean input for 
    proc_normalize_silver and all Gold-layer fact/dimension tables.
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
        order_item_profit_ratio,
        order_item_quantity,
        order_item_gross_total,
        order_item_net_total,
        order_profit_per_order_item,
        order_subregion,
        order_state,
        order_status,
        product_barcode_id,
        product_name,
		product_unit_price,
        shipping_date,
        shipping_time,
        shipping_mode,
        customer_street_number,
        customer_street_name
    )
    SELECT
        /* =======================
           PAYMENT TYPE CLEANING
           ======================= */
        CASE 
            WHEN Type = 'CASH' THEN 'Cash'
            WHEN Type = 'DEBIT' THEN 'Debit'
            WHEN Type = 'TRANSFER' THEN 'Transfer (unspecified)'
            WHEN Type = 'PAYMENT' THEN 'Payment (unspecified)'
            ELSE 'Unknown'
        END AS payment_type,

        [Days for shipping (real)] AS actual_shipping_days,
        [Days for shipment (scheduled)] scheduled_shipping_days,
        [Benefit per order] AS earning_per_order_item,
        [Sales per customer] AS total_sale_per_customer,
        [Delivery Status] AS order_delivery_status,
        [Late_delivery_risk] AS order_late_delivery_risk,
        [Category Id] AS product_category_id,
        [Category Name] AS product_category_name,
        [Customer City] AS customer_city,

        /* =======================
           COUNTRY NORMALIZATION
           ======================= */
        CASE 
            WHEN [Customer Country] IN ('EE. UU.', 'US', 'U.S.') THEN 'United States'
            WHEN [Customer Country] = 'MTxico' THEN 'México'
            WHEN [Customer Country] = 'Panamß' THEN 'Panamá'
            ELSE [Customer Country]
        END AS customer_country,

        CONCAT(COALESCE([Customer Fname], ''), ' ', COALESCE([Customer Lname], '')) AS customer_full_name,
        [Customer Id] AS customer_id,
        [Customer Segment] AS types_of_customers,

        /* ================================
           ⭐ FULL STATE NAME NORMALIZATION
           ================================ */
        CASE 
            WHEN [Customer State] = 'AL' THEN 'Alabama'
            WHEN [Customer State] = 'AZ' THEN 'Arizona'
            WHEN [Customer State] = 'AR' THEN 'Arkansas'
            WHEN [Customer State] = 'CA' THEN 'California'
            WHEN [Customer State] = 'CO' THEN 'Colorado'
            WHEN [Customer State] = 'CT' THEN 'Connecticut'
            WHEN [Customer State] = 'DC' THEN 'District of Columbia'
            WHEN [Customer State] = 'DE' THEN 'Delaware'
            WHEN [Customer State] = 'FL' THEN 'Florida'
            WHEN [Customer State] = 'GA' THEN 'Georgia'
            WHEN [Customer State] = 'HI' THEN 'Hawaii'
            WHEN [Customer State] = 'ID' THEN 'Idaho'
            WHEN [Customer State] = 'IL' THEN 'Illinois'
            WHEN [Customer State] = 'IN' THEN 'Indiana'
            WHEN [Customer State] = 'IA' THEN 'Iowa'
            WHEN [Customer State] = 'KS' THEN 'Kansas'
            WHEN [Customer State] = 'KY' THEN 'Kentucky'
            WHEN [Customer State] = 'LA' THEN 'Louisiana'
            WHEN [Customer State] = 'MA' THEN 'Massachusetts'
            WHEN [Customer State] = 'MD' THEN 'Maryland'
            WHEN [Customer State] = 'MI' THEN 'Michigan'
            WHEN [Customer State] = 'MN' THEN 'Minnesota'
            WHEN [Customer State] = 'MO' THEN 'Missouri'
            WHEN [Customer State] = 'MT' THEN 'Montana'
            WHEN [Customer State] = 'NC' THEN 'North Carolina'
            WHEN [Customer State] = 'ND' THEN 'North Dakota'
            WHEN [Customer State] = 'NE' THEN 'Nebraska'
            WHEN [Customer State] = 'NH' THEN 'New Hampshire'
            WHEN [Customer State] = 'NJ' THEN 'New Jersey'
            WHEN [Customer State] = 'NM' THEN 'New Mexico'
            WHEN [Customer State] = 'NV' THEN 'Nevada'
            WHEN [Customer State] = 'NY' THEN 'New York'
            WHEN [Customer State] = 'OH' THEN 'Ohio'
            WHEN [Customer State] = 'OK' THEN 'Oklahoma'
            WHEN [Customer State] = 'OR' THEN 'Oregon'
            WHEN [Customer State] = 'PA' THEN 'Pennsylvania'
            WHEN [Customer State] = 'PR' THEN 'Puerto Rico'
            WHEN [Customer State] = 'RI' THEN 'Rhode Island'
            WHEN [Customer State] = 'SC' THEN 'South Carolina'
            WHEN [Customer State] = 'SD' THEN 'South Dakota'
            WHEN [Customer State] = 'TN' THEN 'Tennessee'
            WHEN [Customer State] = 'TX' THEN 'Texas'
            WHEN [Customer State] = 'UT' THEN 'Utah'
            WHEN [Customer State] = 'VA' THEN 'Virginia'
            WHEN [Customer State] = 'VT' THEN 'Vermont'
            WHEN [Customer State] = 'WA' THEN 'Washington'
            WHEN [Customer State] = 'WI' THEN 'Wisconsin'
            WHEN [Customer State] = 'WV' THEN 'West Virginia'
            WHEN [Customer State] = 'WY' THEN 'Wyoming'
            ELSE [Customer State]
        END AS customer_state,

        [Customer Street] AS customer_full_street,
        [Department Id] AS store_department_id,
        [Department Name] AS store_department_name,
        [Latitude] AS store_location_latitude,
        [Longitude] AS store_location_longitude,

        CASE WHEN Market = 'LATAM' THEN 'Latin America'
             WHEN Market = 'USCA' THEN 'North America'
             ELSE Market END AS order_continent,

        [Order City] AS order_city,
        [Order Country] AS order_city,
        [Order Customer Id] AS order_customer_id,

        /* ===========================
           SAFE DATE PARSING (ORDER)
           =========================== */
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

        [Order Id]  AS order_id,
        ROUND([Order Item Discount], 1) AS order_item_discount,
        ROUND([Order Item Discount Rate], 2) AS order_item_discount_percentage,
        [Order Item Id] AS order_item_id,
        [Order Item Profit Ratio] AS order_item_profit_ratio,
        [Order Item Quantity] AS order_item_quantity,
        ROUND([Sales], 1) AS order_item_gross_total,
        ROUND([Order Item Total], 1) AS order_item_net_total,
        ROUND([Order Profit Per Order], 1) AS order_profit_per_order_item,

        [Order Region] AS order_subregion,
        [Order State] AS order_state,

        /* ============================================
           ⭐ FINAL ORDER STATUS STANDARDIZATION ⭐
           ============================================ */
        CASE 
            WHEN LOWER([Order Status]) LIKE '%cancel%' THEN 'Canceled'

            WHEN LOWER([Order Status]) IN (
                'pending_payment', 'payment_review', 'pending', 'on_hold'
            ) THEN 'Pending'

            WHEN LOWER([Order Status]) = 'processing' THEN 'Processing'

            WHEN LOWER([Order Status]) IN ('complete', 'closed') THEN 'Completed'

            WHEN LOWER([Order Status]) = 'suspected_fraud' THEN 'Fraud/Error'

            ELSE 'Other'
        END AS order_status,

        [Product Card Id] AS product_barcode_id,
        [Product Name] AS product_name,
		ROUND([Product Price], 1) AS product_unit_price,

        /* ===========================
           SAFE DATE PARSING (SHIPPING)
           =========================== */
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

        [Shipping Mode] AS shipping_mode,

        /* ===========================
           CUSTOMER STREET SPLIT
           =========================== */
        CASE WHEN CHARINDEX(' ', [Customer Street]) > 0
             THEN LEFT([Customer Street], CHARINDEX(' ', [Customer Street]) - 1)
             ELSE NULL END AS customer_street_number,

        CASE WHEN CHARINDEX(' ', [Customer Street]) > 0
             THEN SUBSTRING([Customer Street], CHARINDEX(' ', [Customer Street]) + 1, LEN([Customer Street]))
             ELSE NULL END AS customer_street_name

    FROM bronze.dataco_supply_chain;

    PRINT 'Silver Load Completed Successfully (Safe Mode)!';
END;
GO
