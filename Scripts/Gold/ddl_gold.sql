/*=====================================================================
    GOLD LAYER – STAR SCHEMA (FINAL CLEAN VERSION)
    Project: Profit Drivers Simulator
    DB     : Supply_Chain_Datawarehouse
=====================================================================*/

USE Supply_Chain_Datawarehouse;
GO

/*===========================================================
    0. ENSURE GOLD SCHEMA
===========================================================*/
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

/*===========================================================
    1. DIMENSIONS
===========================================================*/

-----------------------------------------------------------
-- 1.1 DIM CUSTOMER
--    One row per customer_id
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_customer;
GO

CREATE TABLE gold.dim_customer (
    customer_key           INT IDENTITY(1,1) PRIMARY KEY,  -- surrogate key
    customer_id            INT,                            -- natural key from source
    customer_full_name     VARCHAR(255),
    customer_city          VARCHAR(100),
    customer_country       VARCHAR(100),
    customer_state         VARCHAR(100),
    customer_full_street   VARCHAR(200),
    customer_street_number VARCHAR(255),
    customer_street_name   VARCHAR(255),
    types_of_customers     VARCHAR(50)                     -- segment: Consumer, Corporate, etc.
);
GO

INSERT INTO gold.dim_customer (
    customer_id,
    customer_full_name,
    customer_city,
    customer_country,
    customer_state,
    customer_full_street,
    customer_street_number,
    customer_street_name,
    types_of_customers
)
SELECT DISTINCT
    customer_id,
    customer_full_name,
    customer_city,
    customer_country,
    customer_state,
    customer_full_street,
    customer_street_number,
    customer_street_name,
    types_of_customers
FROM silver.dataco_supply_chain
WHERE customer_id IS NOT NULL;
GO


-----------------------------------------------------------
-- 1.2 DIM PRODUCT
--    One row per product_barcode_id
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_product;
GO

CREATE TABLE gold.dim_product (
    product_key           INT IDENTITY(1,1) PRIMARY KEY,  -- surrogate key
    product_barcode_id    INT,                            -- natural key from source
    product_name          VARCHAR(255),
    product_category_id   INT,
    product_category_name VARCHAR(150),
    store_department_id   INT,
    store_department_name VARCHAR(150)
);
GO

INSERT INTO gold.dim_product (
    product_barcode_id,
    product_name,
    product_category_id,
    product_category_name,
    store_department_id,
    store_department_name
)
SELECT DISTINCT
    product_barcode_id,
    product_name,
    product_category_id,
    product_category_name,
    store_department_id,
    store_department_name
FROM silver.dataco_supply_chain
WHERE product_barcode_id IS NOT NULL;
GO


-----------------------------------------------------------
-- 1.3 DIM ORDER LOCATION
--    One row per (continent, country, state, city, subregion, zipcode)
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_order_location;
GO

CREATE TABLE gold.dim_order_location (
    order_location_key INT IDENTITY(1,1) PRIMARY KEY,
    order_continent    VARCHAR(50),
    order_country      VARCHAR(100),
    order_state        VARCHAR(100),
    order_city         VARCHAR(100),
    order_subregion    VARCHAR(100)
);
GO

INSERT INTO gold.dim_order_location (
    order_continent,
    order_country,
    order_state,
    order_city,
    order_subregion
)
SELECT DISTINCT
    order_continent,
    order_country,
    order_state,
    order_city,
    order_subregion
FROM silver.dataco_supply_chain
WHERE order_country IS NOT NULL;
GO


-----------------------------------------------------------
-- 1.4 DIM DATE
--    Master calendar for order_date and shipping_date
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key     INT IDENTITY(1,1) PRIMARY KEY,  -- surrogate key
    full_date    DATE,                           -- actual date
    year_number  INT,
    month_number INT,
    month_name   VARCHAR(20),
    week_number  INT,
    weekday_name VARCHAR(20)
);
GO

INSERT INTO gold.dim_date (
    full_date,
    year_number,
    month_number,
    month_name,
    week_number,
    weekday_name
)
SELECT DISTINCT
    d AS full_date,
    YEAR(d)                    AS year_number,
    MONTH(d)                   AS month_number,
    DATENAME(MONTH, d)         AS month_name,
    DATEPART(WEEK, d)          AS week_number,
    DATENAME(WEEKDAY, d)       AS weekday_name
FROM (
    SELECT order_date   AS d FROM silver.dataco_supply_chain
    UNION
    SELECT shipping_date AS d FROM silver.dataco_supply_chain
) AS src
WHERE d IS NOT NULL;
GO


/*===========================================================
    2. FACT TABLES
===========================================================*/

-----------------------------------------------------------
-- 2.1 FACT_ORDERS
--    Grain: 1 row per order_id (order-level)
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_orders;
GO

CREATE TABLE gold.fact_orders (
    order_key                INT IDENTITY(1,1) PRIMARY KEY, -- surrogate fact key

    -- Foreign keys
    customer_key             INT NULL,
    order_location_key       INT NULL,
    order_date_key           INT NULL,
    shipping_date_key        INT NULL,

    -- Natural key
    order_id                 INT,         -- original order id from source

    -- Operational fields (order-level)
    shipping_mode            VARCHAR(50),
    scheduled_shipping_days  INT,
    actual_shipping_days     INT,
    delivery_delay_days      INT,         -- actual - scheduled
    order_delivery_status    VARCHAR(50),
    order_late_delivery_risk INT,         -- late flag

    -- Commercial fields (using line-level columns but constant per order)
    total_sale_per_customer  DECIMAL(18,4),
    order_profit_per_order   DECIMAL(18,4),

    -- Derived KPI
    order_profit_margin      DECIMAL(18,4)  -- profit / total_sale_per_customer
);
GO


-----------------------------------------------------------
-- 2.2 FACT_ORDER_ITEMS
--    Grain: 1 row per order_item_id (line-item)
-----------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_order_items;
GO

CREATE TABLE gold.fact_order_items (
    order_item_key                 INT IDENTITY(1,1) PRIMARY KEY, -- surrogate key

    -- Foreign keys
    order_key                      INT NULL,   -- link to fact_orders
    product_key                    INT NULL,   -- link to dim_product

    -- Natural line key
    order_item_id                  INT,

    -- Item-level commercial metrics
    product_unit_price             DECIMAL(18,4),
    order_item_quantity            INT,
    order_item_gross_total         DECIMAL(18,4),
    order_item_net_total           DECIMAL(18,4),
    order_item_discount            DECIMAL(18,4),
    order_item_discount_percentage DECIMAL(18,4),
    order_item_profit_ratio        FLOAT
);
GO


/*===========================================================
    3. POPULATE FACT_ORDERS (AGG ORDER-LEVEL)
===========================================================*/

;WITH per_order AS (
    SELECT
        s.order_id,

        -- choose representative values (same across all items per order)
        MIN(s.order_date)               AS order_date,
        MIN(s.shipping_date)            AS shipping_date,
        MIN(s.shipping_mode)            AS shipping_mode,
        MIN(s.customer_id)              AS customer_id,
        MIN(s.order_continent)          AS order_continent,
        MIN(s.order_country)            AS order_country,
        MIN(s.order_state)              AS order_state,
        MIN(s.order_city)               AS order_city,
        MIN(s.order_subregion)          AS order_subregion,

        MIN(s.scheduled_shipping_days)  AS scheduled_shipping_days,
        MIN(s.actual_shipping_days)     AS actual_shipping_days,
        MIN(s.order_delivery_status)    AS order_delivery_status,
        MIN(s.order_late_delivery_risk) AS order_late_delivery_risk,

        -- These are already order-level in the dataset (repeated per line)
        MIN(s.total_sale_per_customer)      AS total_sale_per_customer,
        MIN(s.order_profit_per_order_item)  AS order_profit_per_order

    FROM silver.dataco_supply_chain s
    WHERE s.order_id IS NOT NULL
    GROUP BY s.order_id
)
INSERT INTO gold.fact_orders (
    customer_key,
    order_location_key,
    order_date_key,
    shipping_date_key,
    order_id,
    shipping_mode,
    scheduled_shipping_days,
    actual_shipping_days,
    delivery_delay_days,
    order_delivery_status,
    order_late_delivery_risk,
    total_sale_per_customer,
    order_profit_per_order,
    order_profit_margin
)
SELECT
    dc.customer_key,
    dl.order_location_key,
    od.date_key AS order_date_key,
    sd.date_key AS shipping_date_key,

    p.order_id,
    p.shipping_mode,
    p.scheduled_shipping_days,
    p.actual_shipping_days,
    (p.actual_shipping_days - p.scheduled_shipping_days) AS delivery_delay_days,
    p.order_delivery_status,
    p.order_late_delivery_risk,
    p.total_sale_per_customer,
    p.order_profit_per_order,
    CASE 
        WHEN p.total_sale_per_customer > 0 
             THEN CAST(p.order_profit_per_order AS DECIMAL(18,4)) 
                  / CAST(p.total_sale_per_customer AS DECIMAL(18,4))
        ELSE NULL
    END AS order_profit_margin
FROM per_order p
LEFT JOIN gold.dim_customer dc
       ON p.customer_id = dc.customer_id
LEFT JOIN gold.dim_order_location dl
       ON p.order_continent = dl.order_continent
      AND p.order_country   = dl.order_country
      AND p.order_state     = dl.order_state
      AND p.order_city      = dl.order_city
      AND p.order_subregion = dl.order_subregion
LEFT JOIN gold.dim_date od
       ON p.order_date = od.full_date
LEFT JOIN gold.dim_date sd
       ON p.shipping_date = sd.full_date;
GO


/*===========================================================
    4. POPULATE FACT_ORDER_ITEMS (LINE LEVEL)
===========================================================*/

INSERT INTO gold.fact_order_items (
    order_key,
    product_key,
    order_item_id,
    product_unit_price,
    order_item_quantity,
    order_item_gross_total,
    order_item_net_total,
    order_item_discount,
    order_item_discount_percentage,
    order_item_profit_ratio
)
SELECT
    fo.order_key,
    dp.product_key,
    s.order_item_id,
    s.product_unit_price,
    s.order_item_quantity,
    s.order_item_gross_total,
    s.order_item_net_total,
    s.order_item_discount,
    s.order_item_discount_percentage,
    s.order_item_profit_ratio
FROM silver.dataco_supply_chain s
LEFT JOIN gold.fact_orders fo 
       ON s.order_id = fo.order_id
LEFT JOIN gold.dim_product dp
       ON s.product_barcode_id = dp.product_barcode_id
WHERE s.order_item_id IS NOT NULL;
GO


/*===========================================================
    5. (OPTIONAL) ADD FOREIGN KEYS
    You can comment these out if you get constraint errors
===========================================================*/

ALTER TABLE gold.fact_orders
    ADD CONSTRAINT fk_fact_orders_customer
        FOREIGN KEY (customer_key) 
        REFERENCES gold.dim_customer(customer_key);

ALTER TABLE gold.fact_orders
    ADD CONSTRAINT fk_fact_orders_location
        FOREIGN KEY (order_location_key)
        REFERENCES gold.dim_order_location(order_location_key);

ALTER TABLE gold.fact_orders
    ADD CONSTRAINT fk_fact_orders_orderdate
        FOREIGN KEY (order_date_key)
        REFERENCES gold.dim_date(date_key);

ALTER TABLE gold.fact_orders
    ADD CONSTRAINT fk_fact_orders_shipdate
        FOREIGN KEY (shipping_date_key)
        REFERENCES gold.dim_date(date_key);


ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_items_order
        FOREIGN KEY (order_key)
        REFERENCES gold.fact_orders(order_key);

ALTER TABLE gold.fact_order_items
    ADD CONSTRAINT fk_fact_items_product
        FOREIGN KEY (product_key)
        REFERENCES gold.dim_product(product_key);
GO


PRINT '-----------------------------------------------------------';
PRINT ' GOLD LAYER CREATED SUCCESSFULLY (2 FACTS + 4 DIMENSIONS) ';
PRINT '-----------------------------------------------------------';
GO
