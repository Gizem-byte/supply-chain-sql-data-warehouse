/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Purpose:
    • Defines the standardized structure for the Silver layer.
    • Column order mirrors Bronze for easy comparison.
    • Inline comments indicate original Bronze column names.
    • Column lineage also stored in metadata.column_mapping_simple.
    • Data transformations handled in proc_load_silver.
===============================================================================
*/

-- =======================================================================
-- Drop table if exists
-- =======================================================================
IF OBJECT_ID('silver.dataco_supply_chain','U') IS NOT NULL
BEGIN
    PRINT 'Dropping existing table: silver.dataco_supply_chain_cleaned';
    DROP TABLE silver.dataco_supply_chain;
END
GO

-- =======================================================================
-- Create Silver Table (Cleaned & Standardized)
-- =======================================================================
CREATE TABLE silver.dataco_supply_chain (
    payment_type                    VARCHAR(50),        -- from [Type]
    actual_shipping_days             INT,                -- from [Days for shipping (real)]
    scheduled_shipping_days          INT,                -- from [Days for shipment (scheduled)]
    earning_per_order_item           DECIMAL(18,4),      -- from [Benefit per order]
    total_sale_per_customer          DECIMAL(18,4),      -- from [Sales per customer]
    order_delivery_status            VARCHAR(50),        -- from [Delivery Status]
    order_late_delivery_risk         INT,                -- from [Late_delivery_risk]
    product_category_id              INT,                -- from [Category Id]
    product_category_name            VARCHAR(150),       -- from [Category Name]
    customer_city                    VARCHAR(100),       -- from [Customer City]
    customer_country                 VARCHAR(100),       -- from [Customer Country]
    customer_full_name               VARCHAR(255),       -- derived from [Customer Fname] + [Customer Lname]
    customer_id                      INT,                -- from [Customer Id]
    types_of_customers               VARCHAR(50),        -- from [Customer Segment]
    customer_state                   VARCHAR(100),       -- from [Customer State]
    customer_full_street             VARCHAR(200),       -- from [Customer Street]
    store_department_id              INT,                -- from [Department Id]
    store_department_name            VARCHAR(150),       -- from [Department Name]
    store_location_latitude          DECIMAL(10,6),      -- from [Latitude]
    store_location_longitude         DECIMAL(10,6),      -- from [Longitude]
    order_continent                  VARCHAR(50),        -- from [Market]
    order_city                       VARCHAR(100),       -- from [Order City]
    order_country                    VARCHAR(100),       -- from [Order Country]
    order_customer_id                INT,                -- from [Order Customer Id]
    order_date                       DATE,               -- derived from [order date (DateOrders)]
    order_time                       VARCHAR(5),         -- derived from [order date (DateOrders)]
    order_id                         INT,                -- from [Order Id]
    order_item_discount              FLOAT,              -- from [Order Item Discount]
    order_item_discount_percentage   FLOAT,              -- from [Order Item Discount Rate]
    order_item_id                    INT,                -- from [Order Item Id]
    product_unit_price               DECIMAL(18,4),      -- from [Product Price]
    order_item_profit_ratio          FLOAT,              -- from [Order Item Profit Ratio]
    order_item_quantity              INT,                -- from [Order Item Quantity]
    order_item_gross_total           DECIMAL(18,4),      -- from [Sales]
    order_item_net_total             DECIMAL(18,4),      -- from [Order Item Total]
    order_profit_per_order_item      DECIMAL(18,4),      -- from [Order Profit Per Order]
    order_subregion                  VARCHAR(100),       -- from [Order Region]
    order_state                      VARCHAR(100),       -- from [Order State]
    order_status                     VARCHAR(50),        -- from [Order Status]
    product_barcode_id               INT,                -- from [Product Card Id]
    product_name                     VARCHAR(255),       -- from [Product Name]
    shipping_date                    DATE,               -- from [shipping date (DateOrders)]
    shipping_time                    VARCHAR(5),         -- derived from [shipping date (DateOrders)]
    shipping_mode                    VARCHAR(50),        -- from [Shipping Mode]
    customer_street_number           VARCHAR(255),       -- newly derived from [Customer Street]
    customer_street_name             VARCHAR(255),       -- newly derived from [Customer Street]
    ingestion_timestamp              DATETIME2 DEFAULT SYSDATETIME()  -- load timestamp
);
GO

PRINT 'Created table: silver.dataco_supply_chain';
PRINT '-----------------------------------------------------------';
PRINT 'Silver Layer DDL executed successfully.';
GO
