/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables

-- SILVER LAYER: Normalized entity tables (DROP IF EXISTS before create)
-- Columns kept exactly as in original flat table

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




