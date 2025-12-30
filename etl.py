import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ------------------------------------------------------------
# Args (add RUN_DT for incremental daily processing)
# ------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RAW_DATABASE",     # e.g. ecomm_raw_db
        "S3_OUTPUT_BASE",   # e.g. s3://redshift-analytics-wbai0614/curated
        "RUN_DT",           # e.g. 2025-12-30
    ],
)

RAW_DATABASE = args["RAW_DATABASE"]
S3_OUTPUT_BASE = args["S3_OUTPUT_BASE"].rstrip("/")
RUN_DT = args["RUN_DT"]  # YYYY-MM-DD

# ------------------------------------------------------------
# Context
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Helpful defaults
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

def s3_path(*parts: str) -> str:
    return "/".join([S3_OUTPUT_BASE, *[p.strip("/") for p in parts]])

# ------------------------------------------------------------
# Read from Glue Catalog (RAW)
# ------------------------------------------------------------
# Orders: incremental (RUN_DT only)
orders = spark.table(f"{RAW_DATABASE}.raw_orders").filter(F.col("dt") == RUN_DT)

# Customers/Products: dimension tables (keep static; do NOT rewrite daily)
# If you DO want to refresh dims sometimes, run a separate "weekly" job.
customers = spark.table(f"{RAW_DATABASE}.raw_customers")
products = spark.table(f"{RAW_DATABASE}.raw_products")

# ------------------------------------------------------------
# Transform: ORDERS (Redshift-friendly Parquet + fixed column order)
# - store order_timestamp and order_date as STRING for Parquet compatibility with Redshift
# - enforce explicit column order (Redshift COPY maps Parquet columns by position)
# ------------------------------------------------------------
orders_clean = (
    orders
    .withColumn("order_ts", F.to_timestamp("order_timestamp", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_timestamp", F.date_format(F.col("order_ts"), "yyyy-MM-dd HH:mm:ss"))  # string
    .withColumn("order_date", F.date_format(F.col("order_ts"), "yyyy-MM-dd"))                # string
    .drop("order_ts")
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
    .withColumn("dt", F.col("dt").cast("string"))
)

orders_t = orders_clean.filter(
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("order_timestamp").isNotNull()) &
    (F.col("quantity").isNotNull()) &
    (F.col("quantity") > 0)
)

# ✅ enforce column order
orders_t = orders_t.select(
    "order_id",
    "customer_id",
    "product_id",
    "order_timestamp",
    "order_date",
    "quantity",
    "order_status",
    "payment_method",
    "dt",
)

# ------------------------------------------------------------
# Transform: CUSTOMERS (ONLY used for first-time backfill / occasional refresh)
# Your raw_customers may be col0..col3 if header inference failed
# We'll handle both cases safely.
# ------------------------------------------------------------
customer_cols = set(customers.columns)
if {"customer_id", "signup_date", "country", "customer_segment"}.issubset(customer_cols):
    customers_df = customers.select("customer_id", "signup_date", "country", "customer_segment")
else:
    customers_df = customers.select(
        F.col("col0").alias("customer_id"),
        F.col("col1").alias("signup_date"),
        F.col("col2").alias("country"),
        F.col("col3").alias("customer_segment"),
    )

customers_t = (
    customers_df
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("customer_segment", F.trim(F.col("customer_segment")))
    .filter((F.col("customer_id").isNotNull()) & (F.col("customer_id") != "customer_id"))  # drop header row if present
    .dropDuplicates(["customer_id"])
)

# ------------------------------------------------------------
# Transform: PRODUCTS (ONLY used for first-time backfill / occasional refresh)
# Also drops header row if present.
# ------------------------------------------------------------
products_t = (
    products
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .filter((F.col("product_id").isNotNull()) & (F.col("product_id") != "product_id"))  # drop header row
    .dropDuplicates(["product_id"])
)

# ------------------------------------------------------------
# Write: curated zone
# Orders: write ONLY this dt partition (incremental)
# Dimensions: by default, do NOT rewrite daily (cost + churn). Uncomment if needed.
# ------------------------------------------------------------
(
    orders_t
    .write
    .mode("overwrite")           # overwrite ONLY dt=RUN_DT partition under dynamic partition mode
    .partitionBy("dt")
    .parquet(s3_path("orders"))
)

# OPTIONAL DIM REFRESH (run occasionally, not daily)
# (customers_t.write.mode("overwrite").parquet(s3_path("customers")))
# (products_t.write.mode("overwrite").parquet(s3_path("products")))

job.commit()
