# Brazilian E-Commerce Data Warehouse (Olist Dataset)

**End-to-End Data Engineering Project** – From Raw CSV to Production-Ready Star Schema  

**Dataset**          : Olist Brazilian E-Commerce Public Dataset (Kaggle)  
**Fact table rows**  : 112,650 sales lines  
**Tech Stack**       : Python (Pandas + pyodbc) • Microsoft SQL Server • T-SQL  


## Architecture (Simple & Optimized Star Schema)

![Project Architecture](architecture_diagram.png)

**Why one fact table?**  
Faster queries, simpler joins, ×10 better performance compared to multi-fact designs.

## Folder Structure

```text
brazilian-ecommerce-data-warehouse/
│
├── notebooks/
│   └── eda.ipynb                      # Initial EDA with Python (Pandas + Seaborn)
│
├── scripts/
│   ├── create_staging.sql             # Creates STAGING schema + raw tables
│   ├── load_staging.py                # Loads all 9 CSV files into SQL Server
│   ├── eda_procedures.sql             # Advanced EDA + data quality checks
│   └── create_warehouse.sql           # Builds final star schema + fact_sales
│
├── data/                              # Raw CSV files (optional – not uploaded)
└── README.md                          # This file
```
## How to Run (Step-by-Step)

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/brazilian-ecommerce-data-warehouse.git

2. **Create STAGING schema**
Run scripts/create_staging.sql in SQL Server

3. **Load raw data**
Update path in load_staging.py
Run:Bashpython scripts/load_staging.py

4. **Run EDA & Quality Checks (optional but recommended)**
EXEC scripts/eda_procedures.sql

5. **Build final warehouse**
EXEC scripts/create_warehouse.sql
→ Creates clean dimensions + WAREHOUSE.fact_sales

6. **Connect Power BI / Looker / Tableau**
. Point to WAREHOUSE.fact_sales + dimensions
. Start building dashboards!

## Key Results (from `fact_sales`)

|                      Metric                     |        Value            |
|:-----------------------------------------------:|:-----------------------:|
|                 **Total Revenue**               |     **R$ 15.6M**        |
|                  **Total Orders**               |      **99,441**         |
|               **Total Sales Lines**             |     **112,650**         |
|             **Most Popular Category**           |  **bed_bath_table**     |
|                   **Top State**                 |   **SP (São Paulo)**    |
|            **Average Delivery Time**            |     **12.5 days**       |
|              **Late Delivery Rate**             |       **8.1%**          |
|             **Average Order Value**             |      **R$ 138**         |

What Makes This Project Special

Real-world ETL pipeline (not just Jupyter notebook) 
Proper staging → warehouse separation
High-performance single fact table design (best practice for this dataset)
Full data quality procedures with T-SQL
Clean, documented code
Ready for production or job interviews

This project proves I can:

Design efficient data models
Write clean, maintainable SQL & Python
Handle real-world messy data
Build end-to-end pipelines

Made by Nour Ashraf Mohamed – 2025
LinkedIn: www.linkedin.com/in/nour-ashraf-064686244

# brazilian-ecommerce-data-warehouse
>>>>>>> cb772034895d5493ea51fa665d0fbdea1811058c
