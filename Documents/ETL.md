## ⚙️ ETL Design Overview

The ETL (Extract, Transform, Load) process forms the backbone of the **Data Engineering Phase** in this project.  
It defines how raw operational data is **extracted, transformed, and loaded** into the SQL Data Warehouse following the **Medallion Architecture** (Bronze → Silver → Gold).

![ETL Process]("C:\Users\gizem\Desktop\DATACO PROJECT DONE\ETL.png")

---

### 🟢 **Extraction Phase**

| Component | Method Used | Description |
|------------|--------------|--------------|
| **Extraction Method** | **Pull Extraction** | Data is actively fetched from source files into the warehouse (rather than pushed by source systems). |
| **Extraction Type** | **Full Extraction** | The entire dataset is reloaded at each batch, ensuring consistency with the source CSV files. |
| **Extraction Technique** | **File Parsing** | Data is read and parsed directly from CSV files using SQL Server `BULK INSERT` into the Bronze layer. |

**Tools & Commands Used:**
- `BULK INSERT` for ingestion  
- Truncate-and-reload logic to maintain synchronization  
- CSV input from *DataCo Supply Chain* dataset  

---

### 🔴 **Transformation Phase**

All transformation logic was applied in the **Silver Layer** through stored procedures.

| Subprocess | Description |
|-------------|-------------|
| **Data Cleansing** | Handled duplicates, missing values, invalid dates, inconsistent states, and negative numeric values. |
| **Data Standardization** | Renamed columns, standardized date/time formats, harmonized categorical labels (e.g., countries, states). |
| **Derived Columns** | Created calculated fields such as `delivery_delay_days`, `profit_margin`, `is_late`, `canceled_flag`. |
| **Business Rules & Logic** | Applied transformations for discount rounding, state normalization, and profit sanity checks. |
| **Data Normalization** | Split one wide dataset into multiple entity tables (`customers`, `products`, `orders`, `transactions`). |
| **Data Aggregation** | Prepared summary tables for market, mode, and segment-level performance analysis. |

**Transformation Implemented Via:**  
- SQL Server Stored Procedures (`proc_load_silver.sql`, `proc_normalize_silver.sql`)  
- Business validation queries and automated check scripts (Sense, Logical, Distribution)  

---

### 🔵 **Load Phase**

| Component | Implementation | Description |
|------------|----------------|--------------|
| **Processing Type** | **Batch Processing** | All transformations are executed in scheduled or manual batches rather than real-time streaming. |
| **Load Method** | **Full Load** | The target tables are truncated and fully reloaded each cycle (`TRUNCATE + INSERT`). |
| **Slowly Changing Dimensions (SCD)** | **SCD Type 1 – Overwrite** | Existing records are overwritten with the latest values, as historical versioning was not required. |

**Data Loading Targets:**
- Bronze → Silver → Gold transitions  
- Gold layer built as **SQL Views** for flexibility and analytical integration  

---

### ✅ **Summary**

| ETL Phase | Approach Used |
|------------|---------------|
| **Extraction Method** | Pull |
| **Extraction Type** | Full |
| **Extraction Technique** | File Parsing |
| **Transformation Coverage** | Complete (Cleansing, Standardization, Derivation, Business Logic) |
| **Processing Type** | Batch |
| **Load Method** | Full Load (Truncate + Insert) |
| **SCD Handling** | Type 1 (Overwrite) |

---

📘 *This ETL framework ensures that every pipeline stage — from raw ingestion to analytical delivery — is transparent, reproducible, and optimized for clean, reliable KPI analysis in the Gold Layer.*
