# 🚀 Profit Drivers Simulator  
**End-to-End Data Engineering & Analytics Project (SQL + Data Modeling + Simulation)**  
📊 *Dataset: DataCo Supply Chain* | 🏢 *Industry Context: E-Commerce & Quick-Commerce (Flink-like model)*

---

## 🎯 Project Overview
E-commerce profitability is driven by both **operational efficiency** (delivery performance, cancellations)  
and **commercial levers** (discounts, basket size, product mix).  

These forces are deeply interdependent:  
> High discounts can increase orders but slow deliveries; faster shipping improves satisfaction but raises cost.

This project builds an end-to-end analytics ecosystem to answer:  
> **“If we deliver faster but reduce discounts slightly, does overall profit go up or down?”**

It integrates:
1. A **SQL-based Medallion data warehouse** (Bronze → Silver → Gold)  
2. **Analytical KPI development** for profitability and efficiency  
3. An **interactive Profit Drivers Simulator** for strategic decision-making  

---

## 🧱 Architecture & Tech Stack

| Layer | Purpose | Key Technologies |
|--------|----------|------------------|
| **Bronze** | Raw ingestion layer – original CSV load, schema & sense checks | SQL Server / BULK INSERT |
| **Silver** | Cleansed + normalized data – logic validation, transformations | SQL Server Stored Procedures |
| **Gold** | Analytics model (star schema) for dashboards & simulation | SQL Views / Star Schema Design |
| **Simulator** | Interactive profitability “what-if” analysis | Tableau / Power BI / Streamlit |

🗂️ **Repository Structure**
