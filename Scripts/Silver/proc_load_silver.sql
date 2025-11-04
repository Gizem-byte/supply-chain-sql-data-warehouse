/*
===============================================================================
Stored Procedure: proc_load_silver
===============================================================================
Purpose:
    • Load cleaned and standardized data from Bronze to Silver layer.
    • Aligned with ddl_silver.sql structure.
    • Includes full column mapping lineage and business cleaning logic:
        - Normalizes payment types
        - Cleans country names
        - Expands state codes to full names
        - Derives full name, date, time, and street components
===============================================================================
*/

CREATE OR ALTER PROCEDURE proc_load_silver AS
BEGIN
    PRINT '===========================================================';
    PRINT 'Starting load from Bronze to Silver';
    PRINT '===========================================================';

    TRUNCATE TABLE silver.dataco_supply_chain;

    INSERT INTO silver.dataco_supply_chain (
        payment_type,
        actual_shipping_days,
        scheduled_delivery_days,
        earning_per_order,
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
        order_profit_per_order,
        order_subregion,
        order_state,
        order_status,
        order_zipcode,
        product_barcode_id,
        product_name,
        shipping_date,
        shipping_mode,
        customer_street_number,
        customer_street_name
    )

    SELECT
        -- Payment Type cleanup
        CASE 
            WHEN [Type] = 'CASH' THEN 'Cash'
            WHEN [Type] = 'DEBIT' THEN 'Debit'
            WHEN [Type] = 'TRANSFER' THEN 'Transfer (unspecified)'
            WHEN [Type] = 'PAYMENT' THEN 'Payment (unspecified)'
            ELSE 'Unknown'
        END AS payment_type,               -- from [Type]

        [Days for shipping (real)] AS actual_shipping_days,                  -- from [Days for shipping (real)]
        [Days for shipment (scheduled)] AS scheduled_delivery_days,          -- from [Days for shipment (scheduled)]
        [Benefit per order] AS earning_per_order,                            -- from [Benefit per order]
        [Sales per customer] AS total_sale_per_customer,                     -- from [Sales per customer]
        [Delivery Status] AS order_delivery_status,                          -- from [Delivery Status]
        [Late_delivery_risk] AS order_late_delivery_risk,                    -- from [Late_delivery_risk]
        [Category Id] AS product_category_id,                                -- from [Category Id]
        [Category Name] AS product_category_name,                            -- from [Category Name]
        [Customer City] AS customer_city,                                    -- from [Customer City]

        -- Country normalization
        CASE 
            WHEN [Customer Country] = 'EE. UU.' THEN 'United States'
            ELSE [Customer Country]
        END AS customer_country,                                            -- from [Customer Country]

        -- Derived full name
        CONCAT(COALESCE([Customer Fname], ''), ' ', COALESCE([Customer Lname], '')) AS customer_full_name,  -- derived from [Customer Fname] + [Customer Lname]

        [Customer Id] AS customer_id,                                       -- from [Customer Id]
        [Customer Segment] AS types_of_customers,                           -- from [Customer Segment]

        -- Customer state normalization
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
        END AS customer_state,                                              -- from [Customer State]

        [Customer Street] AS customer_full_street,                          -- from [Customer Street]
        [Customer Zipcode] AS customer_zipcode,                             -- from [Customer Zipcode]
        [Department Id] AS store_department_id,                             -- from [Department Id]
        [Department Name] AS store_department_name,                         -- from [Department Name]
        [Latitude] AS store_location_latitude,                              -- from [Latitude]
        [Longitude] AS store_location_longitude,                            -- from [Longitude]

        -- Market normalization
        CASE 
            WHEN Market = 'LATAM' THEN 'Latin America'
            WHEN Market = 'USCA' THEN 'North America'
            ELSE Market
        END AS order_continent,                                             -- from [Market]

        [Order City] AS order_city,                                         -- from [Order City]
        [Order Country] AS order_country,                                   -- from [Order Country]
        [Order Customer Id] AS order_customer_id,                           -- from [Order Customer Id]

        -- Order date & time derived from text
        TRY_CONVERT(DATE, [order date (DateOrders)], 101) AS order_date,     -- derived from [order date (DateOrders)]
        FORMAT(TRY_CONVERT(DATETIME, [order date (DateOrders)], 101), 'HH:mm') AS order_time, -- derived from [order date (DateOrders)]

        [Order Id] AS order_id,                                             -- from [Order Id]
        [Order Item Cardprod Id] AS order_item_product_barcode_id,          -- from [Order Item Cardprod Id]

        CAST(ROUND([Order Item Discount], 1) AS FLOAT) AS order_item_discount,              -- from [Order Item Discount]
        CAST(ROUND([Order Item Discount Rate], 2) AS FLOAT) AS order_item_discount_percentage, -- from [Order Item Discount Rate]

        [Order Item Id] AS order_item_id,                                   -- from [Order Item Id]
        [Product Price] AS product_unit_price,                              -- from [Product Price]
        [Order Item Profit Ratio] AS order_item_profit_ratio,               -- from [Order Item Profit Ratio]
        [Order Item Quantity] AS order_item_quantity,                       -- from [Order Item Quantity]
        [Sales] AS order_item_gross_total,                                  -- from [Sales]
        [Order Item Total] AS order_item_net_total,                         -- from [Order Item Total]
        [Order Profit Per Order] AS order_profit_per_order,                 -- from [Order Profit Per Order]
        [Order Region] AS order_subregion,                                  -- from [Order Region]
        [Order State] AS order_state,                                       -- from [Order State]
        [Order Status] AS order_status,                                     -- from [Order Status]
        [Order Zipcode] AS order_zipcode,                                   -- from [Order Zipcode]
        [Product Card Id] AS product_barcode_id,                            -- from [Product Card Id]
        [Product Name] AS product_name,                                     -- from [Product Name]

        TRY_CONVERT(DATE, [shipping date (DateOrders)], 101) AS shipping_date, -- from [shipping date (DateOrders)]
        [Shipping Mode] AS shipping_mode,                                   -- from [Shipping Mode]

        -- Street split logic
        CASE 
            WHEN CHARINDEX(' ', [Customer Street]) > 0 
                THEN LEFT([Customer Street], CHARINDEX(' ', [Customer Street]) - 1)
            ELSE NULL
        END AS customer_street_number,                                      -- derived from [Customer Street]

        CASE 
            WHEN CHARINDEX(' ', [Customer Street]) > 0 
                THEN RIGHT([Customer Street], LEN([Customer Street]) - CHARINDEX(' ', [Customer Street]))
            ELSE NULL
        END AS customer_street_name                                         -- derived from [Customer Street]

    FROM bronze.dataco_supply_chain;

    PRINT 'Silver Layer loaded successfully.';
    PRINT '===========================================================';
END;
GO
