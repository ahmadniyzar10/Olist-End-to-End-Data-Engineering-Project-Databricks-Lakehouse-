# Olist End-to-End Data Engineering Project (Databricks Lakehouse)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange?logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID%20Tables-blue?logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-Big%20Data-red?logo=apachespark&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboards-yellow?logo=powerbi&logoColor=white)
![Python](https://img.shields.io/badge/Python-Scripting-green?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Transformations-lightgrey?logo=postgresql&logoColor=white)

---

## Quick Links
- [Notebooks](databricks-olist-e2e/Notebooks/)
- [Job Definition JSON](databricks-olist-e2e/jobs/olist_end_to_end.json)
- [Architecture Diagram](databricks-olist-e2e/End-to-End%20Architecture.png)
- [Lineage Graph](databricks-olist-e2e/dlt_lineage.png)


---

## Project Overview
This project demonstrates an **end-to-end data engineering pipeline** using the **Olist E-Commerce dataset** on **Databricks Free Edition**.  
It is designed as a **portfolio showcase**, highlighting:

- Ingestion, cleaning, and transformation using **Delta Live Tables (DLT)**  
- Advanced **Slowly Changing Dimensions (SCD Type 2)** for dimension tracking  
- Job orchestration with **Databricks Workflows**  
- Gold KPIs materialized as both **Live Views** (for lineage) and **Snapshot Tables** (for BI)  
- Visualization via **Databricks SQL dashboards** and/or **Power BI**  

---

## Project Structure

databricks-olist-e2e/ <br>
├─ notebooks/ <br>
│ ├─ bronze_silver.sql # Bronze → Silver transformations (DLT) <br>
│ ├─ gold_kpis.sql # Gold KPIs (Live Views + Snapshots) <br>
│ ├─ 02_scd2_helpers.py # Python helper functions for SCD2 <br>
│ ├─ 03_post_dlt_scd2.py # Post-DLT notebook: SCD2 merges <br>
│ └─ 04_gold_snapshots.py # (Optional) Additional snapshotting <br>
├─ jobs/ <br>
│ └─ olist_end_to_end.json # Job definition (pipeline + notebooks) <br>
├─ docs/ <br>
│ ├─ architecture.png # Lakehouse architecture diagram <br>
│ └─ data_dictionary.pdf # Original Olist schema <br>
└─ README.md

---

##  End-to-End Architecture
<img width="300" height="500" alt="image" src="https://github.com/user-attachments/assets/9d97c99b-5d29-4498-83db-7bcfae71afa2" />

---

## Pipeline Lineage
<img width="682" height="738" alt="dlt_lineage" src="https://github.com/user-attachments/assets/f73dc340-8341-4d48-9362-d7e055ac41fe" />



---

## Pipeline Layers
- **Bronze**: Raw ingested Olist CSV files (orders, items, customers, products, payments, reviews)  
- **Silver**: Cleaned & standardized tables (data types, joins, null handling)  
- **Facts**: `fact_orders`, `fact_order_items`, `fact_payments`, `fact_reviews`  
- **Dimensions (SCD2)**: `dim_customer`, `dim_product`, `dim_seller`  
- **Gold Views**: KPIs such as Orders, GMV, AOV, Avg Delivery Days, Review Score, Conversion Rate, Revenue by Category  
- **Gold Snapshots**: Materialized Delta tables (for BI dashboards)  

---

## Tech Stack
- **Databricks** (DLT, SQL, PySpark, Workflows)  
- **Delta Lake** (incremental, ACID transactions, SCD2)  
- **Unity Catalog** (storage + governance)  
- **Power BI / Databricks SQL** (visualization layer)  

---

## Key Features
- **Delta Live Tables** for declarative ETL  
- **SCD2 Implementation** via Python helper notebook  
- **End-to-End Job Orchestration** (pipeline → SCD2 → gold snapshots)  
- **Portfolio-ready dashboards** showing business KPIs  

---

## Example KPIs
1. **Total Orders**  
2. **Gross Merchandise Value (GMV)**  
3. **Average Order Value (AOV)**  
4. **Average Delivery Days**  
5. **% Late Deliveries**  
6. **Average Review Score**  
7. **Paid → Delivered Conversion Rate**  
8. **Revenue by Category**  

---

## Dashboard Example
- **Databricks SQL Dashboard**: Interactive filters (date, category)  
- **Power BI Dashboard**: Orders trend, GMV by category, review quality, conversion funnel  

*(Screenshots can be added here once you publish dashboards.)*

---

## How to Run
1. Import the notebooks into Databricks (`/Users/<your_email>/Olist_Project/`)  
2. Create a DLT Pipeline (`olist_dlt_pipeline`) using `bronze_silver.sql` + `gold_kpis.sql`  
3. Create a Job with 3 tasks:
   - **Task 1**: Run pipeline `olist_dlt_pipeline`  
   - **Task 2**: Run `03_post_dlt_scd2.py` (builds SCD2 dims)  
   - **Task 3**: Run `04_gold_snapshots.py` (writes BI tables)  
4. Run the Job (`olist_end_to_end`)  
5. Connect Power BI → Databricks SQL Warehouse → Query snapshot tables  

---

## Dataset
- **Olist E-Commerce Dataset** (Brazilian e-commerce transactions)  
- Public dataset widely used for analytics case studies  

---

## Learning Outcomes
- Build a **production-style pipeline** with Delta Live Tables  
- Manage **slowly changing dimensions** in Delta Lake  
- Automate workflows with **Databricks Jobs**  
- Serve **KPIs to BI tools** with gold snapshot tables
- Documenting pipelines and preparing portfolio-ready artifacts (README, architecture diagram, dashboards)

---

## License
This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.
