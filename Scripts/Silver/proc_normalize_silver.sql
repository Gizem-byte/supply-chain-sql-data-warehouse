/*
====================================================================================
Stored Procedure: proc_normalize_silver
------------------------------------------------------------------------------------
Purpose:
    Safely rebuild and normalize the Silver layer.

Steps:
    1. Drop any old Silver normalized tables (customer, product, shipping, orders, transactions)
    2. Recreate clean empty versions with correct structures
    3. Populate them directly from silver.dataco_supply_chain
    4. Show progress at every step
====================================================================================
*/

CREATE OR ALTER PROCEDURE proc_normalize_silver AS
BEGIN
    SET NOCOUNT ON;

    PRINT '=========================================';
    PRINT '🚀 Starting Safe Rebuild + Normalization';
    PRINT '=========================================';

    /* =======================================================
       STEP 1️⃣ - DROP OLD TABLES
    ======================================================= */
    PRINT '🧹 Dropping old Silver tables if they exist...';

    DROP TABLE IF EXISTS silver.order_transaction;
    DROP TABLE IF EXISTS silver.orders;
    DROP TABLE IF EXISTS silver.shipping;
    DROP TABLE IF EXISTS silver.product;
    DROP TABLE IF EXISTS silver.customer;

    PRINT '✅ Old tables dropped successfully.';


    /* =======================================================
       STEP 2️⃣ - CREATE CLEAN TABLE STRUCTURES
    ======================================================= */
    PRINT '📦 Creating clean Silver entity tables...';

    CREATE TABLE silver.customer (
        customer_id INT PRIMARY KEY,
        customer_full_name VARCHAR(255),
        customer_city VARCHAR(100),
        customer_state VARCHAR(100),
        customer_country VARCHAR(100),
        customer_zipcode INT,
        types_of_customers VARCHAR(50)
    );

    CREATE TABLE silver.product (
        product_barcode_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        product_category_id INT,
        product_category_name VARCHAR(150),
        store_department_id INT,
        store_department_name VARCHAR(150),
        product_unit_price DECIMAL(18,2),
        store_location_latitude DECIMAL(10,6),
        store_location_longitude DECIMAL(10,6)
    );

    CREATE TABLE silver.shipping (
        shipping_mode VARCHAR(50) PRIMARY KEY,
        scheduled_delivery_days INT,
        actual_shipping_days INT,
        order_late_delivery_risk INT,
        order_delivery_status VARCHAR(50)
    );

    CREATE TABLE silver.orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        order_time VARCHAR(5),
        order_state VARCHAR(100),
        order_status VARCHAR(50),
        order_zipcode DECIMAL(12,0),
        order_subregion VARCHAR(100),
        order_country VARCHAR(100),
        order_city VARCHAR(100),
        order_continent VARCHAR(50)
    );

    CREATE TABLE silver.order_transaction (
        order_item_id INT PRIMARY KEY,
        order_id INT,
        product_barcode_id INT,
        shipping_mode VARCHAR(50),
        payment_type VARCHAR(50),
        order_item_quantity INT,
        order_item_discount DECIMAL(18,2),
        order_item_discount_percentage DECIMAL(18,2),
        order_item_gross_total DECIMAL(18,2),
        order_item_net_total DECIMAL(18,2),
        order_profit_per_order DECIMAL(18,2),
        earning_per_order DECIMAL(18,2),
        total_sale_per_customer DECIMAL(18,2),
        late_delivery_flag INT,
        shipping_date DATE,
        shipping_time VARCHAR(5)
    );

    PRINT '✅ Clean table structures created.';


    /* =======================================================
       STEP 3️⃣ - POPULATE TABLES
    ======================================================= */
    PRINT '🚚 Populating data into entity tables...';

    -- Customer
    INSERT INTO silver.customer
    SELECT DISTINCT
        customer_id,
        customer_full_name,
        customer_city,
        customer_state,
        customer_country,
        customer_zipcode,
        types_of_customers
    FROM silver.dataco_supply_chain
    WHERE customer_id IS NOT NULL;

    PRINT '✅ silver.customer populated.';


    -- Product
    INSERT INTO silver.product
    SELECT DISTINCT
        product_barcode_id,
        product_name,
        product_category_id,
        product_category_name,
        store_department_id,
        store_department_name,
        product_unit_price,
        store_location_latitude,
        store_location_longitude
    FROM silver.dataco_supply_chain
    WHERE product_barcode_id IS NOT NULL;

    PRINT '✅ silver.product populated.';


    -- Shipping
    INSERT INTO silver.shipping
    SELECT DISTINCT
        shipping_mode,
        scheduled_delivery_days,
        actual_shipping_days,
        order_late_delivery_risk,
        order_delivery_status
    FROM silver.dataco_supply_chain
    WHERE shipping_mode IS NOT NULL;

    PRINT '✅ silver.shipping populated.';


    -- Orders
    INSERT INTO silver.orders
    SELECT DISTINCT
        order_id,
        customer_id,
        order_date,
        order_time,
        order_state,
        order_status,
        order_zipcode,
        order_subregion,
        order_country,
        order_city,
        order_continent
    FROM silver.dataco_supply_chain
    WHERE order_id IS NOT NULL;

    PRINT '✅ silver.orders populated.';


    -- Order Transactions
    INSERT INTO silver.order_transaction (
        order_item_id,
        order_id,
        product_barcode_id,
        shipping_mode,
        payment_type,
        order_item_quantity,
        order_item_discount,
        order_item_discount_percentage,
        order_item_gross_total,
        order_item_net_total,
        order_profit_per_order,
        earning_per_order,
        total_sale_per_customer,
        late_delivery_flag,
        shipping_date,
        shipping_time
    )
    SELECT
        order_item_id,
        order_id,
        product_barcode_id,
        shipping_mode,
        payment_type,
        order_item_quantity,
        order_item_discount,
        order_item_discount_percentage,
        order_item_gross_total,
        order_item_net_total,
        order_profit_per_order,
        earning_per_order,
        total_sale_per_customer,
        order_late_delivery_risk,
        shipping_date,
        shipping_time
    FROM silver.dataco_supply_chain
    WHERE order_item_id IS NOT NULL;

    PRINT '✅ silver.order_transaction populated.';


    PRINT '=========================================';
    PRINT '🎯 Rebuild + Normalization Completed!';
    PRINT '=========================================';
END;
GO
