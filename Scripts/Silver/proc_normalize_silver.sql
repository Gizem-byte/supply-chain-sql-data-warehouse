/*=====================================================================
   PROCEDURE: proc_normalize_silver
   PURPOSE  : Normalize silver.dataco_supply_chain into atomic entity tables
              (customer, product, orders, order_item, delivery_info)
   AUTHOR   : Gizem
   STATUS   : ✅ FINAL VERSION WITH SEPARATE DELIVERY_INFO
=====================================================================*/

CREATE OR ALTER PROCEDURE proc_normalize_silver AS
BEGIN
    SET NOCOUNT ON;

    PRINT '🚀 Starting Silver Normalization (with separate delivery_info)';
    PRINT '===================================================================';

    /* =======================================================
       1️⃣ Drop old normalized tables if they exist
    ======================================================= */
    DROP TABLE IF EXISTS silver.order_item;
    DROP TABLE IF EXISTS silver.orders;
    DROP TABLE IF EXISTS silver.product;
    DROP TABLE IF EXISTS silver.customer;
    DROP TABLE IF EXISTS silver.shipping_info;

    PRINT '🧹 Old Silver tables dropped. Creating new normalized structures...';


    /* =======================================================
       2️⃣ Create normalized Silver tables
    ======================================================= */

    /* -------------------------
       CUSTOMER TABLE
       Grain: One row per unique customer
    ------------------------- */
    CREATE TABLE silver.customer (
        customer_id INT PRIMARY KEY,             -- Unique ID for each customer
        customer_full_name VARCHAR(255),         -- Combined first + last name
        customer_city VARCHAR(100),              -- City of residence
        customer_state VARCHAR(100),             -- State of residence
        customer_country VARCHAR(100),           -- Country
        types_of_customers VARCHAR(50)           -- Customer segment (e.g., Consumer, Corporate)
    );


    /* -------------------------
       PRODUCT TABLE
       Grain: One row per unique product_barcode_id
       Deduplication used (ROW_NUMBER) since same product sold many times
    ------------------------- */
    CREATE TABLE silver.product (
        product_barcode_id INT PRIMARY KEY,      -- Unique product identifier (barcode)
        product_name VARCHAR(255),               -- Product name
        product_category_id INT,                 -- Product category ID
        product_category_name VARCHAR(150),      -- Product category label
        store_department_id INT,                 -- Store department code
        store_department_name VARCHAR(150),      -- Store department name
        product_unit_price DECIMAL(18,4),        -- Unit price of the product
        store_location_latitude DECIMAL(10,6),   -- Latitude coordinate of store
        store_location_longitude DECIMAL(10,6)   -- Longitude coordinate of store
    );


    /* -------------------------
       ORDERS TABLE
       Grain: One row per unique order_id
       Represents order-level financial and customer linkage
    ------------------------- */
    CREATE TABLE silver.orders (
        order_id INT PRIMARY KEY,                -- Unique ID per order
        customer_id INT,                         -- FK → silver.customer
        payment_type VARCHAR(50),                -- Payment method (Cash, Debit, etc.)
        order_date DATE,                         -- Order placement date
        order_time VARCHAR(5),                   -- Order placement time
        order_state VARCHAR(100),                -- State where delivery occurred
        order_status VARCHAR(50),                -- Order status (Delivered, Canceled)
        order_subregion VARCHAR(100),            -- Geographic subregion
        order_country VARCHAR(100),              -- Country of delivery
        order_city VARCHAR(100),                 -- City of delivery
        order_continent VARCHAR(50)              -- Continent (USCA, LATAM, etc.)
    );


    /* -------------------------
       DELIVERY INFO TABLE
       Grain: One row per order (1:1 with orders)
       Represents shipping / delivery performance
    ------------------------- */
    CREATE TABLE silver.shipping_info (
        order_id INT PRIMARY KEY,                -- FK → silver.orders
        shipping_mode VARCHAR(50),               -- Shipping type (Standard, Express, etc.)
        scheduled_shipping_days INT,             -- Planned delivery duration
        actual_shipping_days INT,                -- Actual delivery duration
        order_late_delivery_risk INT,            -- 0/1 late flag
        order_delivery_status VARCHAR(50),       -- Text status (Late, On Time, etc.)
        shipping_date DATE,                      -- Shipment dispatch date
        shipping_time VARCHAR(5)                 -- Dispatch time (HH:MM)
    );


    /* -------------------------
       ORDER ITEM TABLE
       Grain: One row per product line within an order
       Represents transactional & profitability details
    ------------------------- */
    CREATE TABLE silver.order_item (
        order_item_id INT PRIMARY KEY,           -- Unique item line ID
        order_id INT,                            -- FK → silver.orders
        product_barcode_id INT,                  -- FK → silver.product
        order_item_quantity INT,                 -- Quantity sold of this product
        order_item_discount DECIMAL(18,4),       -- Discount amount €
        order_item_discount_percentage DECIMAL(18,4), -- Discount percentage
        order_item_gross_total DECIMAL(18,4),    -- Gross amount (before discount)
        order_item_net_total DECIMAL(18,4),      -- Net amount (after discount)
        order_profit_per_order_item DECIMAL(18,4), -- Profit per product line (renamed)
        earning_per_order_item DECIMAL(18,4),      -- Benefit per product line (renamed)
        total_sale_per_customer DECIMAL(18,4)      -- Lifetime customer total (for aggregation)
    );

    PRINT 'Clean normalized structures created.';
    PRINT '----------------------------------------------------';


    /* =======================================================
       Populate normalized tables
    ======================================================= */

    PRINT 'Populating CUSTOMER...';
    INSERT INTO silver.customer
    SELECT DISTINCT
        customer_id,
        customer_full_name,
        customer_city,
        customer_state,
        customer_country,
        types_of_customers
    FROM silver.dataco_supply_chain
    WHERE customer_id IS NOT NULL;
    PRINT 'silver.customer populated.';


    PRINT 'Populating PRODUCT (deduplicated by barcode)...';
    ;WITH product_dedup AS (
        SELECT 
            product_barcode_id,
            product_name,
            product_category_id,
            product_category_name,
            store_department_id,
            store_department_name,
            product_unit_price,
            store_location_latitude,
            store_location_longitude,
            ROW_NUMBER() OVER (
                PARTITION BY product_barcode_id 
                ORDER BY product_name
            ) AS rn
        FROM silver.dataco_supply_chain
        WHERE product_barcode_id IS NOT NULL
    )
    INSERT INTO silver.product
    SELECT 
        product_barcode_id,
        product_name,
        product_category_id,
        product_category_name,
        store_department_id,
        store_department_name,
        product_unit_price,
        store_location_latitude,
        store_location_longitude
    FROM product_dedup
    WHERE rn = 1;
    PRINT 'silver.product populated (deduplicated).';


    PRINT 'Populating ORDERS...';
    INSERT INTO silver.orders
    SELECT DISTINCT
        order_id,
        customer_id,
        payment_type,
        order_date,
        order_time,
		order_subregion,
        order_state,
        order_status,
        order_country,
        order_city,
        order_continent
    FROM silver.dataco_supply_chain
    WHERE order_id IS NOT NULL;
    PRINT 'silver.orders populated.';


    PRINT 'Populating DELIVERY_INFO...';
    INSERT INTO silver.shipping_info
    SELECT DISTINCT
        order_id,
        shipping_mode,
        scheduled_shipping_days,
        actual_shipping_days,
        order_late_delivery_risk,
        order_delivery_status,
        shipping_date,
        shipping_time
    FROM silver.dataco_supply_chain
    WHERE order_id IS NOT NULL;
    PRINT 'silver.shipping_info populated.';


    PRINT 'Populating ORDER_ITEM...';
    INSERT INTO silver.order_item
    SELECT
        order_item_id,
        order_id,
        product_barcode_id,
        order_item_quantity,
        order_item_discount,
        order_item_discount_percentage,
        order_item_gross_total,
        order_item_net_total,
        order_profit_per_order_item,
        earning_per_order_item,
        total_sale_per_customer
    FROM silver.dataco_supply_chain
    WHERE order_item_id IS NOT NULL;
    PRINT 'silver.order_item populated.';


    PRINT '===================================================================';
    PRINT 'Silver Normalization Completed Successfully (with delivery_info)';
    PRINT '===================================================================';
END;
GO
