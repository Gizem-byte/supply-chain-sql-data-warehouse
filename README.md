## 🧱 Project Structure & Phases

### 🧭 **Project: Profit Drivers Simulator**
An **end-to-end data engineering and analytics project** using the **DataCo Supply Chain** dataset —  
modeled around **Flink’s grocery delivery business context**.

---

### 🧩 **Phase 1 — Data Engineering (SQL Warehouse)**

**Objective:** Build a scalable SQL-based data warehouse following the **Medallion Architecture** (Bronze → Silver → Gold).

**Key Deliverables:**
- Designed and implemented schemas for **Bronze, Silver, and Gold layers**.  
- Created **DDL scripts, stored procedures, and ETL pipelines** for data ingestion and transformation.  
- Applied **data quality controls** — including **Sense Check**, **Logical Check**, and **Distribution Profiling**.  
- Normalized the dataset into clean relational structures.  
- Built a **Star Schema** in the Gold layer supporting profitability and efficiency KPIs.

---

### 📊 **Phase 2 — Data Analysis & KPI Development**

**Objective:** Transform cleansed data into business-ready metrics to uncover profitability drivers.

**Key Deliverables:**
- Defined analytical KPIs such as:
  - `Late Delivery Rate`
  - `Cancellation Rate`
  - `Profit Margin %`
  - `Average Basket Size`
  - `Discount–Profit Correlation`
- Developed **SQL analytical views and summary tables** for trend analysis.  
- Created an **insight-ready Gold layer** to power dashboards and scenario simulation.  
- Conducted **exploratory analysis** to connect operational and commercial performance indicators.

---

### 🧮 **Phase 3 — Profitability Simulator**

**Objective:** Build an **interactive simulation dashboard** for business stakeholders to test “what-if” scenarios.

**Key Features:**
- Adjustable scenario parameters (e.g.,  
  ↓ Late deliveries by 10%, ↑ Basket size by €3, ↓ Discounts by 5%).  
- Dynamic recalculation of profitability, delivery efficiency, and margin impact.  
- Integrated directly with **Gold Layer views** for real-time responsiveness.  
- Built using **Tableau / Power BI / Streamlit**.

---

### 🎯 **Outcome**

Delivered a realistic **business case** demonstrating how **operational efficiency**  
and **commercial levers** jointly impact **profitability**.  

This project showcases:
- The complete **data lifecycle** — from raw ingestion to business simulation.  
- Strong alignment with **real-world logistics and quick-commerce operations**.  
- Technical mastery in **SQL Data Engineering**, **Analytical Modeling**, and **Data Visualization**.

---
