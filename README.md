# Retail Sales Data Engineering Pipeline using Azure Databricks

## Project Overview

This project demonstrates the design and implementation of an end-to-end Azure Data Engineering pipeline using the Medallion Architecture (Bronze, Silver, and Gold layers). The pipeline ingests raw retail sales data from Azure Data Lake Storage (ADLS Gen2), performs data profiling and transformation using PySpark in Azure Databricks, stores processed data in Delta Lake using Unity Catalog, creates analytical fact and dimension tables, and automates the complete workflow using Databricks Workflows.

The project follows industry best practices for data engineering, including data quality validation, dimensional modeling, Delta Lake implementation, and workflow orchestration.

---

## Business Objective

The objective of this project is to build a scalable, automated, and analytics-ready data pipeline that transforms raw retail sales data into structured datasets for reporting and business intelligence.

The processed data can be consumed by Power BI or other BI tools to support business decision-making.

---

## Dataset

**Dataset:** Global Superstore Dataset

**Source:** Kaggle

**Total Records:** 51,290

The dataset contains:

- Customer Information
- Product Information
- Sales Transactions
- Shipping Details
- Profit
- Discount
- Regional Information

---

# Technology Stack

- Microsoft Azure
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks
- Unity Catalog
- Delta Lake
- PySpark
- Databricks Workflows
- GitHub

---

# Project Architecture

                           Retail Sales CSV Dataset
                                      │
                                      ▼
                     Azure Data Lake Storage Gen2 (ADLS)
                           (Bronze / Raw Data Layer)
                                      │
                                      ▼
                          Azure Databricks (PySpark)
                                      │
                ┌─────────────────────┴─────────────────────┐
                │                                           │
                ▼                                           ▼
      Bronze Layer                              Data Profiling & Quality Checks
 (Raw Data Ingestion)                    (Nulls, Duplicates, Schema Validation)
                │
                ▼
                 Silver Layer (Data Cleaning & Transformation)
      ├── Data Type Conversion
      ├── Trim Text Columns
      ├── Shipping_Days
      ├── Order_Year
      ├── Order_Month
      ├── Order_Quarter
      └── Profit_Margin
                │
                ▼
        Delta Lake + Unity Catalog (Sales_Silver)
                │
                ▼
                 Gold Layer (Dimensional Model)
      ├── FactSales
      ├── DimCustomer
      ├── DimProduct
      ├── DimLocation
      └── DimDate
                │
                ▼
          Databricks Workflow Automation
                │
                ▼
         Power BI Dashboard (Future Reporting)

The pipeline follows the Medallion Architecture:

# Medallion Architecture

## Bronze Layer

The Bronze layer stores the raw dataset exactly as received from Azure Data Lake Storage.

### Activities Performed

- Data Ingestion
- Schema Inference
- Data Profiling
- Initial Data Validation

---

## Silver Layer

The Silver layer performs data cleansing and transformation.

### Data Quality Checks

- Duplicate Validation
- Null Value Analysis
- Data Type Validation
- Business Rule Validation
- Shipping Date Validation
- Negative Value Checks

### Transformations

- Data Type Conversion
- Column Name Standardization
- Text Trimming
- Shipping_Days
- Order_Year
- Order_Month
- Order_Quarter
- Profit_Margin

The transformed data is stored as a Delta Table using Unity Catalog.

---

## Gold Layer

The Gold layer implements a Star Schema for analytical reporting.

### Fact Table

- FactSales

### Dimension Tables

- DimCustomer
- DimProduct
- DimLocation
- DimDate

---

# Delta Lake Implementation

The Silver and Gold layers are stored in Delta Lake.

### Benefits

- ACID Transactions
- Schema Enforcement
- Time Travel
- Data Reliability
- High Performance Queries

---

# Unity Catalog

Unity Catalog was used for centralized governance and storage management.

Configured Components:

- Catalog
- Schema
- Storage Credential
- External Location
- Delta Tables

---

# Databricks Workflow

The pipeline was automated using Databricks Workflows.

Workflow Sequence

```

BronzeToSilver Notebook
↓
SilverToGold Notebook

```

The workflow executes both notebooks sequentially using task dependencies.

---

# Star Schema


                 DimCustomer
                      |
DimDate -------- FactSales -------- DimProduct
                      |
                 DimLocation

```

---

# Challenges Faced

### Challenge

While transforming the dataset, Spark generated data type conversion errors because certain rows containing embedded quotation marks were parsed incorrectly.

### Solution

The issue was resolved by configuring the Spark CSV reader using:

- multiLine = true
- quote = '"'
- escape = '"'

This allowed Spark to correctly parse all records without any data loss.

---

# Project Validation

### Silver Layer

- Total Records: **51,290**

### Gold Layer

| Table | Records |
|--------|--------:|
| Sales_Silver | 51,290 |
| FactSales | 51,290 |
| DimCustomer | 1,590 |
| DimProduct | 10,768 |
| DimLocation | 3,819 |
| DimDate | 1,430 |

---

# Project Screenshots

## Unity Catalog – Data Objects

<img width="940" height="428" alt="image" src="https://github.com/user-attachments/assets/c0e5cec1-0641-4971-8f57-9ae4db8f9965" />


---

## Databricks Workflow Execution

<img width="940" height="455" alt="image" src="https://github.com/user-attachments/assets/43b6c666-9714-4cbc-93f3-a73f325511a4" />


---

## Gold Layer Table Validation

<img width="940" height="448" alt="image" src="https://github.com/user-attachments/assets/aba334c6-722d-499d-a65a-bf2b3df3ffca" />


---

# Folder Structure

```

Retail-Data-Engineering-Pipeline/
│
├── notebooks/
│   ├── 01_Bronze_to_Silver_Transformation.py
│   └── 02_Silver_to_Gold_Modeling.py
│
├── diagrams/
│   ├── architecture.png
│   └── star_schema.png
│
├── screenshots/
│   ├── unity_catalog.png
│   ├── workflow_execution.png
│   └── gold_layer_validation.png
│
├── docs/
│   └── Retail_Data_Engineering_Project_Documentation.docx
│
└── README.md

```

---

# Key Skills Demonstrated

- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- Delta Lake
- PySpark
- Data Profiling
- Data Cleaning
- Data Transformation
- Data Quality Validation
- Star Schema Design
- Fact & Dimension Modeling
- Databricks Workflow Automation
- Medallion Architecture

---

# Future Enhancements

- Azure Data Factory Integration
- Incremental Data Loading
- Slowly Changing Dimensions (SCD Type 2)
- Surrogate Keys
- CI/CD Pipeline
- Monitoring & Alerting
- Power BI Dashboard Integration

---

# Conclusion

This project demonstrates a complete Azure Data Engineering solution using Azure Databricks, Delta Lake, Unity Catalog, and Medallion Architecture. It covers the complete lifecycle from raw data ingestion to analytical data modeling and automated pipeline execution, following modern cloud data engineering best practices.

---

## Author

**GK Pavithra**

Aspiring Azure Data Engineer | Data Analyst

**Skills:** Azure Databricks | PySpark | SQL | Python | Power BI | Azure Data Lake | Delta Lake | Unity Catalog
