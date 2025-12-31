# E-Commerce Analytics Pipeline on AWS (Fully Automated)

## Overview
This project implements a **fully automated, production-style data pipeline** for e-commerce analytics using AWS managed services.  
It ingests daily transactional data, performs ETL transformations, loads curated data into **Amazon Redshift Serverless**, and visualizes insights in **Amazon QuickSight**.

The pipeline is **event-driven, idempotent, and scheduled**, requiring no manual intervention.

---

## Architecture

EventBridge (Daily 7:00 AM)
→ AWS Lambda (orchestration + data generation)
→ Amazon S3 (raw layer)
→ AWS Glue (ETL: raw → curated)
→ Amazon S3 (curated Parquet)
→ Amazon Redshift Serverless (fact + dimensions)
→ Amazon QuickSight (dashboards)

---

## Data Flow (End-to-End)

### 1. Daily Trigger
- **Amazon EventBridge Scheduler** runs daily at **7:00 AM**
- Input payload: `{}` (Lambda automatically uses today’s date)