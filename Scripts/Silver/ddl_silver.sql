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


IF OBJECT_ID ('silver.Customer','U') IS NOT NULL
    DROP TABLE silver.Customer;
GO

CREATE TABLE silver.Customer (
    "Customer Id"        INT PRIMARY KEY,
    "Customer Fname"     VARCHAR(100),
    "Customer Lname"     VARCHAR(100),
    "Customer Email"     VARCHAR(150),
    "Customer Password"  VARCHAR(200),
    "Customer Segment"   VARCHAR(50),
    "Customer Street"    VARCHAR(255),
    "Customer City"      VARCHAR(100),
    "Customer State"     VARCHAR(100),
    "Customer Country"   VARCHAR(100),
    "Customer Zipcode"   INT
);
GO

IF OBJECT_ID ('silver.Product','U') IS NOT NULL
    DROP TABLE silver.Product;
GO

CREATE TABLE silver.Product (
    "Product Card Id"     INT PRIMARY KEY,
    "Product Name"        VARCHAR(255),
    "Product Description" VARCHAR(500),
    "Product Image"       VARCHAR(255),
    "Product Category Id" INT,
    "Category Name"       VARCHAR(150),
    "Product Price"       DECIMAL(12,2),
    "Product Status"      INT,
    "Department Id"       INT,
    "Department Name"     VARCHAR(150)
);
GO

IF OBJECT_ID ('silver.Orders','U') IS NOT NULL
    DROP TABLE silver.Orders;
GO

CREATE TABLE silver.Orders (
    "Order Id"                  INT PRIMARY KEY,
    "Order Customer Id"         INT,
    "order date (DateOrders)"   DATE,
    "Order City"                VARCHAR(100),
    "Order State"               VARCHAR(100),
    "Order Country"             VARCHAR(100),
    "Order Zipcode"             DECIMAL(12,0),
    "Order Status"              VARCHAR(50),
    "Order Region"              VARCHAR(100),
    "Market"                    VARCHAR(50)
);
GO

IF OBJECT_ID ('silver.OrderItem','U') IS NOT NULL
    DROP TABLE silver.OrderItem;
GO

CREATE TABLE silver.OrderItem (
    "Order Item Id"               INT PRIMARY KEY,
    "Order Id"                    INT,
    "Product Card Id"             INT,
    "Order Item Quantity"         INT,
    "Order Item Product Price"    DECIMAL(12,2),
    "Order Item Discount"         DECIMAL(12,2),
    "Order Item Discount Rate"    DECIMAL(6,4),
    "Order Item Total"            DECIMAL(12,2),
    "Order Item Profit Ratio"     DECIMAL(6,4),
    "Sales"                       DECIMAL(12,2),
    "Sales per customer"          DECIMAL(12,2),
    "Benefit per order"           DECIMAL(12,2),
    "Order Profit Per Order"      DECIMAL(12,2)
);
GO

IF OBJECT_ID ('silver.Delivery','U') IS NOT NULL
    DROP TABLE silver.Delivery;
GO

CREATE TABLE silver.Delivery (
    "Order Id"                        INT PRIMARY KEY,
    "Days for shipment (scheduled)"   INT,
    "Days for shipping (real)"        INT,
    "Delivery Status"                 VARCHAR(100),
    "Late_delivery_risk"              INT,
    "shipping date (DateOrders)"      DATE,
    "Shipping Mode"                   VARCHAR(100)
);
GO

IF OBJECT_ID ('silver.Category','U') IS NOT NULL
    DROP TABLE silver.Category;
GO

CREATE TABLE silver.Category (
    "Category Id"    INT PRIMARY KEY,
    "Category Name"  VARCHAR(150)
);
GO

IF OBJECT_ID ('silver.Department','U') IS NOT NULL
    DROP TABLE silver.Department;
GO

CREATE TABLE silver.Department (
    "Department Id"   INT PRIMARY KEY,
    "Department Name" VARCHAR(150)
);
GO


IF OBJECT_ID ('silver.tokenized_access_logs','U') IS NOT NULL
    DROP TABLE silver.tokenized_access_logs;
GO

CREATE TABLE silver.tokenized_access_logs (
    "Product"    VARCHAR(255),
    "Category"   VARCHAR(150),
    "Date"       VARCHAR(50),
    "Month"      VARCHAR(50),
    "Hour"       INT,
    "Department" VARCHAR(150),
    "ip"         VARCHAR(100),
    "url"        VARCHAR(500)
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


