# Automated AWS Data Analytics Pipeline (E-Commerce)

## Overview
This project implements a fully automated, production-style data pipeline on AWS using synthetic e-commerce data. The system ingests daily order data, processes it with ETL, loads it into a data warehouse, and visualizes insights in near real-time.

## Architecture
EventBridge → Lambda → S3 (raw) → Glue ETL → S3 (curated) → Redshift Serverless → QuickSight

## Data Flow
1. **EventBridge Scheduler**
   - Runs daily at 7:00 AM
   - Triggers Lambda with `{}` payload

2. **AWS Lambda**
   - Generates daily synthetic order data
   - Uploads CSV to S3 (`raw/orders/dt=YYYY-MM-DD`)
   - Starts Glue ETL with `--RUN_DT`
   - Calls Redshift stored procedure

3. **Amazon S3**
   - Raw zone: CSV files partitioned by date
   - Curated zone: Parquet files partitioned by date

4. **AWS Glue**
   - Transforms raw CSV → curated Parquet
   - Handles schema normalization and partitioning

5. **Amazon Redshift Serverless**
   - Stored procedure `sp_load_orders_daily`
   - Loads curated data into `fact_orders`
   - Deduplicates by `order_id`

6. **Amazon QuickSight**
   - Direct Query mode
   - Dashboards update automatically as new data arrives

## Key Features
- Fully automated daily ingestion
- Idempotent loads (safe re-runs)
- Partitioned S3 storage
- Serverless architecture
- Cost-efficient design
- Production IAM least-privilege policies

## Tables
- `fact_orders`
- `dim_customers`
- `dim_products`
- View: `vw_orders_analytics`

## Scheduling
- EventBridge cron: cron(0 7 * * ? *)
- Timezone: `America/New_York`

## Backfill / Manual Runs
To load a specific date:
```json
{ "run_dt": "2025-12-31" }
