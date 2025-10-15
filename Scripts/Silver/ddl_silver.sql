/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

-- SILVER LAYER: Normalized entity tables (DROP IF EXISTS before create)
-- Column names are chnaged for better readibility and for better understandning

===============================================================================
*/


IF OBJECT_ID ('silver.dataco_supply_chain','U') IS NOT NULL
	DROP TABLE silver.dataco_supply_chain;
GO


CREATE TABLE silver.dataco_supply_chain(
    Type                            VARCHAR(50),
    "Days for shipping (real)"      INT,
    "Days for shipment (scheduled)" INT,
    "Benefit per order"             DECIMAL(12,2),
    "Sales per customer"            DECIMAL(12,2),
    "Delivery Status"               VARCHAR(50),
    "Late_delivery_risk"            INT,
    "Category Id"                   INT,
    "Category Name"                 VARCHAR(150),
    "Customer City"                 VARCHAR(100),
    "Customer Country"              VARCHAR(100),
    "Customer Email"                VARCHAR(150),
    "Customer Fname"                VARCHAR(100),
    "Customer Id"                   INT,
    "Customer Lname"                VARCHAR(100),
    "Customer Password"             VARCHAR(100),
    "Customer Segment"              VARCHAR(50),
    "Customer State"                VARCHAR(100),
    "Customer Street"               VARCHAR(200),
    "Customer Zipcode"              INT,
    "Department Id"                 INT,
    "Department Name"               VARCHAR(150),
    "Latitude"                      DECIMAL(10,6),
    "Longitude"                     DECIMAL(10,6),
    "Market"                        VARCHAR(50),
    "Order City"                    VARCHAR(100),
    "Order Country"                 VARCHAR(100),
    "Order Customer Id"             INT,
    "order date (DateOrders)"       DATE,
    "Order Id"                      INT,
    "Order Item Cardprod Id"        INT,
    "Order Item Discount"           DECIMAL(12,2),
    "Order Item Discount Rate"      DECIMAL(6,4),
    "Order Item Id"                 INT,
    "Order Item Product Price"      DECIMAL(12,2),
    "Order Item Profit Ratio"       DECIMAL(6,4),
    "Order Item Quantity"           INT,
    "Sales"                         DECIMAL(12,2),
    "Order Item Total"              DECIMAL(12,2),
    "Order Profit Per Order"        DECIMAL(12,2),
    "Order Region"                  VARCHAR(100),
    "Order State"                   VARCHAR(100),
    "Order Status"                  VARCHAR(50),
    "Order Zipcode"                 DECIMAL(12,0),
    "Product Card Id"               INT,
    "Product Category Id"           INT,
    "Product Description"           VARCHAR(500),
    "Product Image"                 VARCHAR(255),
    "Product Name"                  VARCHAR(255),
    "Product Price"                 DECIMAL(12,2),
    "Product Status"                INT,
    "shipping date (DateOrders)"    DATE,
    "Shipping Mode"                 VARCHAR(50)
);
GO


IF OBJECT_ID ('silver.tokenized_access_logs','U') IS NOT NULL
	DROP TABLE silver.tokenized_access_logs;
GO

CREATE TABLE silver.tokenized_access_logs (

    "Product"        VARCHAR(255),
    "Category"       VARCHAR(150),
    "Date"           VARCHAR(50),
    "Month"          VARCHAR(50),
    "Hour"           INT,
    "Department"     VARCHAR(150),
    "ip"             VARCHAR(100),
    "url"            VARCHAR(500)
);
GO 

EXEC sp_rename 'silver.dataco_supply_chain.Type', 'payment_type', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Days for shipping (real)', 'actual_shipping_days', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Days for shipment (scheduled)', 'scheduled_delivery_days', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Benefit per order', 'earning_per_order', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Sales per customer', 'total_sale_per_customer', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Benefit per order', 'earning_per_order', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Delivery Status', 'order_delivery_status', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Late_delivery_risk', 'order_late_delivery_risk', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Category Id', 'product_category_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Category Name', 'product_category_name', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer City', 'customer_city', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Country', 'customer_country', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Email', 'customer_email', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Fname', 'customer_first_name', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Id', 'customer_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Lname', 'customer_last_name', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Password', 'customer_password', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Segment', 'types_of_customers', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer State', 'customer_state', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.customer_street', 'customer_full_street', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Customer Zipcode', 'customer_zipcode', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Department Id', 'store_department_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Department Name', 'store_department_name', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Latitude', 'store_location_latitude', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Longitude', 'store_location_longitude', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.order_continent_delivered', 'order_continent', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Region', 'order_subregion', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order City', 'order_city', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Country', 'order_country', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Customer Id', 'order_customer_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.order_date_date', 'order date (DateOrders)', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Id', 'order_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.product_barcode_id', 'order_item_product_barcode_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Discount', 'order_item_discount', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Discount Rate   ', 'order_item_discount_percentage', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Id', 'order_item_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Profit Ratio', 'order_item_profit_ratio', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Quantity', 'order_item_quantity', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Sales', 'order_item_gross_total', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Item Total ', 'order_item_net_total', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Profit Per Order', 'order_profit_per_order', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order State', 'order_state', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Status', 'order_status', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Order Zipcode', 'order_zipcode', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Product Card Id', 'product_barcode_id', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Product Category Id', 'product_category_id_duplicate', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Product Description', 'product_description', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Product Name', 'product_name', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.product_price', 'product_unit_price', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Product Status', 'product_status', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Shipping date (DateOrders)', 'shipping_date', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.Shipping Mode', 'shipping_mode', 'COLUMN';
EXEC sp_rename 'silver.dataco_supply_chain.order_time_new', 'order_time', 'COLUMN';

ALTER TABLE silver.dataco_supply_chain
DROP COLUMN product_status;

ALTER TABLE silver.dataco_supply_chain
DROP COLUMN order_item_unit_price;


ALTER TABLE silver.dataco_supply_chain
DROP COLUMN product_description,product_category_id_duplicate;

ALTER TABLE silver.dataco_supply_chain
DROP COLUMN customer_email,customer_password;

ALTER TABLE silver.dataco_supply_chain
ADD customer_full_name VARCHAR(255);

ALTER TABLE silver.dataco_supply_chain
DROP COLUMN customer_first_name,customer_last_name;

ALTER TABLE silver.dataco_supply_chain
ADD customer_street_number VARCHAR(255),customer_street_name VARCHAR(255);

ALTER TABLE silver.dataco_supply_chain
ALTER COLUMN [order_date_date] DATETIME;

ALTER TABLE silver.dataco_supply_chain
ADD order_date DATE;

ALTER TABLE silver.dataco_supply_chain
ADD order_time_new VARCHAR(5);


UPDATE silver.dataco_supply_chain
SET order_time_new = FORMAT(order_time, 'HH:mm');


ALTER TABLE silver.dataco_supply_chain
DROP COLUMN order_time;


ALTER TABLE silver.dataco_supply_chain
DROP COLUMN "order date (DateOrders)";


ALTER TABLE silver.dataco_supply_chain
ALTER COLUMN order_item_discount FLOAT;

ALTER TABLE silver.dataco_supply_chain
ALTER COLUMN order_item_discount_percentage FLOAT;

ALTER TABLE silver.dataco_supply_chain
DROP COLUMN "Product Image";


