# SQL Data Warehouse with Postgres 🗄️
**Welcome to my Data Warehouse and Analytics Project!** 🚀   


This repository showcases an end-to-end data warehouse built from scratch using **PostgreSQL**, adapted from Baraa Salkini's SQL Server-based YouTube tutorial (https://youtu.be/9GVqKuTVANE?si=HATxStdwdLw7oyb1). It demonstrates modern data engineering practices, including a **medallion architecture**, ETL pipelines, and analytical queries, tailored for Postgres.
The CSV files, data flow and schema diagrams are works of Baraa Salkini. They are for educational use only.

---

## 🏗️ Project Overview
This project builds a scalable data warehouse to consolidate and analyze sales data. Key components include:

- **Data Architecture**: Implements a medallion architecture with **Bronze**, **Silver**, and **Gold** layers.
  - **Bronze**: Raw data ingested from CSV files (ERP and CRM sources).
  - **Silver**: Cleansed, standardized, and normalized data for processing.
  - **Gold**: Star-schema data model optimized for analytics.
- **ETL Pipelines**: Scripts for extracting, transforming, and loading data into Postgres.
- **Data Modeling**: Fact and dimension tables designed for efficient querying.
- **Analytics**: SQL queries to uncover insights on customer behavior/details, product performance, and sales trends.

---

## 🎯 Key Features
- Adapted SQL Server scripts to PostgreSQL.
- Optimized query performance for large datasets using Postgres indexes and partitioning.
- Implemented a star schema in the Gold layer for business-ready analytics.
- Clean, modular SQL scripts with clear documentation.

This project is ideal for showcasing skills in:
- **Data Engineering** (ETL pipelines, data warehousing)
- **SQL Development** (Postgres-specific optimizations)
- **Data Modeling** (star schema, medallion architecture)
- **Data Analytics** (actionable insights)

---

## 🛠️ Tools & Technologies
- **PostgreSQL**: Database for hosting the warehouse.
- **DBeaver**: GUI for database management.
- **GitHub**: Version control and project hosting.

---

## 📂 Repository Structure

```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project 
```

## 🚀 Getting Started
1. **Prerequisites**:
   - Install [PostgreSQL](https://www.postgresql.org/download/) and a GUI.
   - Clone this repository.
2. **Setup**:
   - Create a Postgres database: `CREATE DATABASE warehouse;`.
   - Run scripts in order: `bronze/*.sql`, `silver/*.sql`, `gold/*.sql`.
   - Load sample data from `datasets/` (see `docs/data_catalog.md` for details).
3. **Explore**:
   - Run analytical queries in `gold/queries.sql` for insights.
   - View diagrams in `docs/` for architecture and data flow.

For detailed requirements, see `docs/requirements.md`.

---

## 🔍 Postgres Adaptation
This project was adapted from Baraa's project which was in SQL Server to Postgres, addressing challenges like:
- Converting T-SQL’s `IDENTITY` to Postgres `SERIAL`.
- Adjusting date/time functions (e.g., `GETDATE()` to `NOW()`).
- Optimizing queries with Postgres-specific indexing.

---

## 🙏 Acknowledgments
- Inspired by Baraa Salkini’s [SQL Data Warehouse tutorial]([https://www.youtube.com/watch?v=9GVqKuTVANE&t=14578s]).
- Datasets provided by the tutorial, used with permission for educational purposes.

---

## ☕ Connect
Check out my [LinkedIn](https://www.linkedin.com/in/ridwan-badamasi/) to follow my data engineering journey!  
Feel free to star this repo, share feedback, or suggest improvements.

**License**: MIT 
**Note**: Sample datasets are for demonstration only. Ensure compliance with data usage policies before sharing.
