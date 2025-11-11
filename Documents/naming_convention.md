# 🧾 Naming Conventions — DataCo Supply Chain Project

This document defines the **standard naming conventions** used throughout the **Data Engineering and Analytics Project** built on the **Medallion Architecture (Bronze → Silver → Gold)**.

The goal is to ensure consistency, readability, and scalability across all layers — from raw ingestion to analytical modeling.

---

## 🏗️ 1️⃣ Database & Schema Naming

| Layer | Schema Name | Purpose |
|--------|--------------|----------|
| **Raw Layer** | `bronze` | Stores raw ingested data (1:1 with source CSVs). |
| **Cleansed Layer** | `silver` | Stores cleaned, standardized, and normalized tables. |
| **Analytical Layer** | `gold` | Contains the final reporting structures — fact and dimension views. |
| **Metadata Layer** | `metadata` | Maintains column mappings, lineage, and ETL audit information. |

✅ *All schema names are lowercase for SQL Server consistency and readability.*

---

## 📋 2️⃣ Table Naming Convention

| Type | Format | Example |
|-------|---------|---------|
| **Raw / Source Table** | `<schema>.<source_table>` | `bronze.dataco_supply_chain` |
| **Cleansed Table** | `<schema>.<source_table>` (same name as Bronze) | `silver.dataco_supply_chain` |
| **Normalized Entity Tables** | `<schema>.<entity>` | `silver.customers`, `silver.orders`, `silver.products`, `silver.order_transaction` |
| **Fact Tables (Gold)** | `<schema>.vw_fact_<subject>` | `gold.vw_fact_sales` |
| **Dimension Tables (Gold)** | `<schema>.vw_dim_<entity>` | `gold.vw_dim_customer`, `gold.vw_dim_product`, etc. |

✅ *Fact and Dimension tables are always prefixed with `vw_` to indicate they are **views**, not physical tables.*

---

## 🔢 3️⃣ Column Naming Convention

| Type | Convention | Example |
|------|-------------|----------|
| **General Rule** | `snake_case` (lowercase with underscores) | `order_item_quantity`, `total_sale_per_customer` |
| **Date Columns** | Always end with `_date` | `order_date`, `shipping_date` |
| **Datetime Columns** | Use `_timestamp` | `ingestion_timestamp` |
| **Numeric Measures** | Use descriptive metric names | `order_profit_per_order`, `earning_per_order`, `total_sale_per_customer` |
| **Flags / Booleans** | Prefix with `is_`, `has_`, or suffix with `_flag` | `is_late`, `canceled_flag`, `late_delivery_flag` |
| **Keys** | End with `_id` or `_key` depending on purpose | `customer_id` (business key), `customer_key` (surrogate key) |

✅ *All columns are lowercase and descriptive — abbreviations are avoided unless widely standard (e.g., `id`).*

---

## ⚙️ 4️⃣ Stored Procedures Naming

| Type | Format | Example |
|-------|---------|----------|
| **Load Procedures** | `proc_load_<layer>` | `proc_load_bronze`, `proc_load_silver` |
| **Normalization Procedures** | `proc_normalize_<layer>` | `proc_normalize_silver` |
| **Validation Checks** | `proc_check_<type>` | `proc_check_sense_bronze`, `proc_check_logical_silver` |
| **ETL Orchestration (optional)** | `proc_run_etl_<layer>` | `proc_run_etl_gold` |

✅ *All procedures begin with `proc_` for clear differentiation from tables and views.*

---

## 📊 5️⃣ View Naming (Gold Layer)

| View Type | Naming Convention | Example |
|------------|------------------|----------|
| **Fact View** | `vw_fact_<topic>` | `vw_fact_sales` |
| **Dimension View** | `vw_dim_<entity>` | `vw_dim_customer`, `vw_dim_date` |
| **Analytical / KPI View** | `vw_kpi_<category>` | `vw_kpi_profitability`, `vw_kpi_efficiency` |

✅ *All analytical views start with `vw_` for easy identification.*

---

## 🧱 6️⃣ Metadata Tables

| Table Name | Purpose |
|-------------|----------|
| `metadata.column_mapping_bronze_to_silver` | Tracks renamed, added, and dropped columns between layers. |
| `metadata.etl_audit_log` *(optional)* | Logs ETL execution history — start time, end time, status, row counts, duration. |

✅ *Metadata schema provides transparency and traceability for transformations.*

---

## 🧰 7️⃣ File Naming Convention

| File Type | Format | Example |
|------------|---------|----------|
| **DDL Scripts** | `ddl_<layer>.sql` | `ddl_bronze.sql`, `ddl_silver.sql` |
| **Stored Procedures** | `proc_<purpose>.sql` | `proc_load_silver.sql` |
| **Validation Tests** | `<check_type>_check_<layer>.sql` | `sense_check_bronze.sql`, `logical_test_silver.sql` |
| **Views / Modeling** | `gold_layer_views.sql` | Contains all star schema definitions |
| **Documentation Files** | `data_catalog_<layer>.md` | `data_catalog_gold.md`, `naming_conventions.md` |

✅ *File names follow lowercase + underscore format for portability and readability.*

---

## 🧮 8️⃣ Example Consistency Snapshot

| Object Type | Example Name | Purpose |
|--------------|---------------|----------|
| **Schema** | `silver` | Clean, standardized tables |
| **Table** | `silver.order_transaction` | Holds transaction-level cleansed data |
| **Procedure** | `proc_load_silver` | Loads data from Bronze → Silver |
| **Column** | `order_item_discount_percentage` | Descriptive and consistent business naming |
| **View** | `gold.vw_fact_sales` | Analytical fact view for dashboard KPIs |
| **Metadata** | `metadata.column_mapping_bronze_to_silver` | Tracks column lineage |

---

## ✅ Summary

> Every layer in this project follows **consistent, descriptive, and industry-standard naming practices**.  
> These conventions ensure:
> - Easy collaboration between Data Engineers and Analysts  
> - Seamless traceability across ETL layers  
> - Professional readability in both SQL scripts and GitHub documentation  

---

📘 *Following these conventions not only improves maintainability but also demonstrates strong data architecture discipline — a key expectation for professional data engineering and analytics projects.*
