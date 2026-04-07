# Global Partners — AWS Data Pipeline

A production-grade data engineering pipeline built on AWS that ingests restaurant transaction data from SQL Server, transforms it through a medallion architecture, and serves business insights through an interactive Streamlit dashboard.

---

## Architecture Overview

```
SQL Server (RDS)
      ↓
AWS Glue JDBC Connection
      ↓
S3 Bronze Layer (Raw Parquet)
      ↓
S3 Silver Layer (Cleaned & Validated)
      ↓
S3 Gold Layer (Business Metrics)
      ↓
Amazon Athena (SQL Query Layer)
      ↓
Streamlit Dashboard (EC2)
```

---

## Tech Stack

| Layer | Service | Why |
|-------|---------|-----|
| Source | SQL Server on AWS RDS | Managed relational database, no infrastructure overhead |
| Ingestion | AWS Glue JDBC | Serverless, native SQL Server connector, no custom drivers |
| Storage | Amazon S3 | Scalable, cost-effective data lake storage |
| Transformation | AWS Glue PySpark | Serverless Spark, handles large datasets without cluster management |
| Orchestration | Glue Workflows | Native AWS scheduling, event-based job chaining |
| Query Layer | Amazon Athena | Serverless SQL over S3, pay per query, no data movement |
| Dashboard | Streamlit on EC2 | Python-native, fast to build, integrates directly with Athena |
| Encryption | AWS KMS | Customer-managed keys for fine-grained access control |
| Monitoring | CloudWatch | Native AWS logging and alerting for Glue jobs |
| CI/CD | GitHub Actions | Automated deployment of Glue scripts to S3 on every push |

---

## Project Structure

```
global-partners-pipeline/
├── glue/
│   └── scripts/
│       ├── bronze_job.py        # SQL Server → S3 Bronze (raw ingestion)
│       ├── silver_job.py        # S3 Bronze → S3 Silver (cleaning & validation)
│       └── gold_job.py          # S3 Silver → S3 Gold (business metrics)
├── dashboard/
│   └── dashboard.py             # Streamlit multi-page dashboard
├── athena/
│   └── 01_athena_setup.sql      # Athena external table definitions
├── docs/
│   └── architecture_diagram.svg # Final approved architecture diagram
├── .github/
│   └── workflows/
│       └── deploy.yml           # GitHub Actions CI/CD pipeline
└── README.md
```

---

## Data Pipeline

### Source Tables (SQL Server)
| Table | Records | Description |
|-------|---------|-------------|
| `order_items` | 203,519 | Transactional line items per order |
| `order_item_options` | 193,017 | Add-ons and customizations per line item |
| `date_dim` | varies | Calendar dimension for time-based joins |

### Bronze Layer
- Raw ingestion from SQL Server via JDBC
- No transformations — data lands exactly as it exists in the source
- Stored as Parquet partitioned by ingestion date

### Silver Layer
- Casts all columns to correct data types
- Strips dirty URL values from `item_category`
- Removes rows missing key identifiers
- Deduplicates on natural keys
- Adds derived columns: `order_date`, `line_revenue`, `is_discount`

### Gold Layer — Business Metrics
| Table | Metric |
|-------|--------|
| `fact_daily_clv` | Customer Lifetime Value with High/Medium/Low tiers |
| `fact_rfm_segments` | RFM scores and segment labels (VIP, Churn Risk, etc.) |
| `fact_churn_indicators` | Days since last order, avg gap, spend trend, risk tag |
| `fact_sales_trends` | Daily revenue by location and category with calendar context |
| `fact_loyalty_comparison` | Loyalty vs non-loyalty spend and order metrics |
| `fact_location_performance` | Revenue rank and performance tier per restaurant |
| `fact_discount_effectiveness` | Gross vs net revenue for discounted vs full-price orders |
| `dim_customer` | Master customer record combining CLV + RFM + Churn |

---

## Dashboard Pages

| Page | Business Question |
|------|------------------|
| 🏠 Overview | High-level KPIs — total customers, revenue, CLV, at-risk count |
| 👥 Customer Segmentation | What distinct segments emerge from RFM analysis? |
| ⚠️ Churn Risk | Which customers are at risk of churning? |
| 📈 Sales Trends | What are the monthly and seasonal revenue patterns? |
| 💳 Loyalty Program | How does loyalty membership affect spend and retention? |
| 📍 Location Performance | Which restaurant locations are top vs low performers? |
| 🏷️ Discount Effectiveness | How are discounts affecting revenue and profitability? |

---

## Pipeline Schedule

The full pipeline runs automatically every day at **1:00 AM UTC** via Glue Workflows:

```
Schedule Trigger (1AM UTC)
        ↓
global-partners-bronze (JDBC ingest)
        ↓ on success
global-partners-silver (clean & validate)
        ↓ on success
global-partners-gold (calculate metrics)
```

---

## CI/CD

Every push to the `main` branch automatically deploys updated Glue scripts to S3 via GitHub Actions. See `.github/workflows/deploy.yml` for the workflow definition.

---

## Security

- S3 bucket encrypted with customer-managed KMS key
- All IAM roles follow least-privilege principle
- RDS accessible only within VPC
- S3 VPC endpoint for private Glue → S3 connectivity
- No credentials stored in code — EC2 uses IAM role for Athena/S3 access

---

## Setup

See the phase-by-phase setup guides:
- **Phase 1:** AWS infrastructure (S3, KMS, IAM, Glue connection)
- **Phase 2:** PySpark ETL jobs (Bronze → Silver → Gold)
- **Phase 3:** Glue Workflow orchestration
- **Phase 4:** Athena setup + Streamlit dashboard on EC2
