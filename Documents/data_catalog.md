# 🧾 Gold Layer Data Catalog  
**Project:** Profit Drivers Simulator  
**Layer:** Gold (Analytics-Ready Star Schema)  
**Author:** Gizem  
**Database:** `Supply_Chain_Datawarehouse`

---

## 📘 Overview
The **Gold Layer** provides an analytics-ready **star schema** optimized for business intelligence dashboards and profitability simulation.  
It consists of one **fact table** (`vw_fact_sales`) and five **dimension tables** (`vw_dim_date`, `vw_dim_customer`, `vw_dim_product`, `vw_dim_market`, `vw_dim_order`).

---

## 🌟 1. gold.vw_dim_date
**Purpose:** Stores distinct order dates to enable time-based analytics (daily, monthly, quarterly performance).

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `date_key` | DATE | Unique date identifier (used as FK in fact tables). |
| `year_number` | INT | Calendar year of the order. |
| `month_number` | INT | Calendar month number (1–12). |
| `quarter_number` | INT | Calendar quarter (1–4). |
| `month_name` | NVARCHAR(20) | Full month name (e.g., January). |
| `week_number` | INT | Week of the year. |
| `weekday_name` | NVARCHAR(20) | Day of the week (e.g., Monday). |

---

## 👤 2. gold.vw_dim_customer
**Purpose:** Stores customer demographic and geographic details for segmentation and profitability analysis.

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `customer_key` | INT | Unique key identifying each customer record. |
| `customer_full_name` | NVARCHAR(255) | Full name of the customer. |
| `customer_city` | NVARCHAR(100) | City of the customer. |
| `customer_country` | NVARCHAR(100) | Country of residence. |
| `customer_state` | NVARCHAR(100) | State or region. |
| `customer_segment` | NVARCHAR(50) | Customer type (Consumer, Corporate, Home Office). |

---

## 📦 3. gold.vw_dim_product
**Purpose:** Stores descriptive product information for linking sales and profit data to product lines and categories.

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `product_key` | INT | Unique identifier for the product. |
| `product_name` | NVARCHAR(255) | Product name as recorded in system. |
| `product_category_id` | INT | Internal numeric identifier of category. |
| `product_category_name` | NVARCHAR(150) | Product category name (e.g., Furniture). |
| `store_department_id` | INT | Numeric identifier of department. |
| `store_department_name` | NVARCHAR(150) | Name of the department (e.g., Office Supplies). |

---

## 🌍 4. gold.vw_dim_market
**Purpose:** Captures the geographic and market-level segmentation for regional performance analytics.

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `market_key` | NVARCHAR(50) | Unique market or regional key (e.g., LATAM, Europe). |
| `order_country` | NVARCHAR(100) | Country where the order was placed. |
| `order_subregion` | NVARCHAR(100) | Sub-region of the country (e.g., Western Europe). |
| `order_state` | NVARCHAR(100) | State or province name. |
| `order_city` | NVARCHAR(100) | City or delivery destination. |

---

## 🚚 5. gold.vw_dim_order
**Purpose:** Contains operational and fulfillment-related details of each order (delivery mode, delays, cancellation).

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `order_key` | INT | Unique identifier for the order. |
| `payment_type` | NVARCHAR(50) | Payment method (Cash, Debit, Transfer). |
| `order_status` | NVARCHAR(50) | Current order processing status (Complete, Pending, Canceled). |
| `order_delivery_status` | NVARCHAR(50) | Delivery outcome (Late, On time, etc.). |
| `shipping_mode` | NVARCHAR(50) | Shipping or delivery method (Standard, Express, Same Day). |
| `shipping_speed_category` | NVARCHAR(50) | Speed classification (Standard, Priority, Fast). |
| `is_premium_mode` | BIT | 1 = Premium shipping, 0 = Standard. |
| `actual_shipping_days` | INT | Actual delivery duration in days. |
| `scheduled_delivery_days` | INT | Planned delivery duration in days. |
| `delivery_delay_days` | INT | Actual - Scheduled days difference. |
| `is_late` | BIT | 1 if order delivered late, 0 otherwise. |
| `canceled_flag` | BIT | 1 if delivery was canceled. |
| `order_date_key` | DATE | Date the order was created. |
| `shipping_date_key` | DATE | Date the order was shipped. |

---

## 💰 6. gold.vw_fact_sales
**Purpose:**  
Central fact table capturing all measurable and numeric sales events at order-item level.  
Used to compute KPIs such as revenue, profit, margin, discount rate, and basket size.

| Column Name | Data Type | Description |
|--------------|------------|--------------|
| `sales_key` | INT | Unique transaction (order item) identifier. |
| `order_key` | INT | Foreign key to `dim_order`. |
| `product_key` | INT | Foreign key to `dim_product`. |
| `customer_key` | INT | Foreign key to `dim_customer`. |
| `market_key` | NVARCHAR(50) | Foreign key to `dim_market`. |
| `product_unit_price` | DECIMAL(18,1) | Product unit price at transaction time. |
| `order_item_quantity` | INT | Quantity of items in order line. |
| `order_item_discount` | DECIMAL(18,1) | Discount applied to order line. |
| `order_item_discount_percentage` | DECIMAL(18,2) | Discount percentage applied. |
| `order_item_gross_total` | DECIMAL(18,1) | Total before discount. |
| `order_item_net_total` | DECIMAL(18,1) | Total after discount. |
| `order_profit_per_order` | DECIMAL(18,1) | Profit contribution from order line. |
| `earning_per_order` | DECIMAL(18,1) | Earning or margin per order. |
| `total_sale_per_customer` | DECIMAL(18,1) | Total spend per customer. |
| `profit_margin_percentage` | DECIMAL(18,2) | Profit margin = Profit ÷ Sales × 100. |
| `late_delivery_risk_flag` | BIT | 1 if order was late. |
| `ingestion_timestamp` | DATETIME2 | Timestamp of load into warehouse. |

---

## 🧩 Model Overview

| Table | Type | Grain | Primary Key | Joins To |
|--------|------|--------|--------------|-----------|
| `gold.vw_fact_sales` | Fact | 1 row per order-item transaction | `sales_key` | All dimensions |
| `gold.vw_dim_date` | Dimension | 1 row per unique date | `date_key` | `order_date_key`, `shipping_date_key` |
| `gold.vw_dim_customer` | Dimension | 1 row per customer | `customer_key` | `customer_key` |
| `gold.vw_dim_product` | Dimension | 1 row per product | `product_key` | `product_key` |
| `gold.vw_dim_market` | Dimension | 1 row per region or market | `market_key` | `market_key` |
| `gold.vw_dim_order` | Dimension | 1 row per order | `order_key` | `order_key` |

---

## ✅ Business Use
This Gold layer powers:
- **Operational KPIs** – Late Delivery %, Avg Shipping Time, Cancellation Rate.  
- **Commercial KPIs** – Basket Size, Discount Impact, Profit Margin %.  
- **Profitability Simulation** – “What-If” analysis for delivery speed, discount, and basket growth.

---
