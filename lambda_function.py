import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

import boto3  # available by default in AWS Lambda

random.seed(42)

COUNTRIES = ["US", "CA", "UK", "DE", "FR", "AU", "JP", "IN"]
SEGMENTS = ["Consumer", "Small Business", "Corporate"]
CATEGORIES = ["Electronics", "Furniture", "Stationery", "Kitchen", "Outdoors"]
PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "APPLE_PAY", "GOOGLE_PAY", "BANK_TRANSFER"]
ORDER_STATUS = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "REFUNDED"]  # weighted by repetition


@dataclass
class Config:
    out_dir: str = "synthetic_out"
    start_dt: date = date(2025, 1, 1)
    days: int = 10
    total_orders: int = 100_000
    n_customers: int = 10_000
    n_products: int = 500
    # S3 upload settings
    s3_bucket: Optional[str] = None
    s3_prefix: str = "raw"  # root prefix in bucket, e.g. raw/...


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_csv(path: Path, header: List[str], rows: List[List[str]]) -> None:
    _ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def _random_timestamp(on_date: date) -> str:
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    dt = datetime(on_date.year, on_date.month, on_date.day, hour, minute, second)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_orders_for_date(cfg: Config, day: date, n_orders: int) -> Path:
    """
    Writes a single orders.csv for one day under:
      orders/dt=YYYY-MM-DD/orders.csv
    """
    customer_ids = [f"C{i:06d}" for i in range(1, cfg.n_customers + 1)]
    product_ids = [f"P{i:06d}" for i in range(1, cfg.n_products + 1)]

    rows: List[List[str]] = []
    for i in range(1, n_orders + 1):
        # Order IDs reset daily (fine for demo). If you want global-unique IDs, tell me.
        order_id = f"O{day.strftime('%Y%m%d')}{i:06d}"

        cust = random.choice(customer_ids)
        prod = random.choice(product_ids)
        ts = _random_timestamp(day)
        qty = random.randint(1, 5)
        status = random.choice(ORDER_STATUS)
        pay = random.choice(PAYMENT_METHODS)

        rows.append([order_id, cust, prod, ts, str(qty), status, pay])

    out = Path(cfg.out_dir) / "orders" / f"dt={day.isoformat()}" / "orders.csv"
    _write_csv(
        out,
        ["order_id", "customer_id", "product_id", "order_timestamp", "quantity", "order_status", "payment_method"],
        rows,
    )
    return out


def upload_to_s3(paths: List[Path], cfg: Config) -> None:
    if not cfg.s3_bucket:
        raise ValueError("cfg.s3_bucket is required for S3 upload in Lambda")

    s3 = boto3.client("s3")
    for p in paths:
        rel = p.relative_to(Path(cfg.out_dir))
        key = f"{cfg.s3_prefix}/{rel.as_posix()}"
        print(f"Uploading {p} -> s3://{cfg.s3_bucket}/{key}")
        s3.upload_file(str(p), cfg.s3_bucket, key)


def lambda_handler(event, context):
    """
    Paste this whole file into Lambda's code editor (lambda_function.py)
    and set Handler to: lambda_function.lambda_handler

    Event examples:
      {}  -> uses today's date (Lambda runtime date, UTC)
      {"run_dt": "2025-01-11"} -> uses provided date

    Environment variables (Lambda Configuration -> Environment variables):
      BUCKET_NAME (required) e.g. redshift-analytics-wbai0614
      DAILY_ORDERS (optional) default 10000
      S3_PREFIX (optional) default raw
    """
    import os

    run_dt = (event or {}).get("run_dt")
    if run_dt:
        day = date.fromisoformat(run_dt)
    else:
        day = date.today()

    bucket = os.environ["BUCKET_NAME"]
    daily_orders = int(os.environ.get("DAILY_ORDERS", "10000"))
    s3_prefix = os.environ.get("S3_PREFIX", "raw")

    # Lambda can only write to /tmp
    cfg = Config(
        out_dir="/tmp/synthetic_out",
        start_dt=day,
        days=1,
        total_orders=daily_orders,
        n_customers=10_000,
        n_products=500,
        s3_bucket=bucket,
        s3_prefix=s3_prefix,
    )

    orders_path = generate_orders_for_date(cfg, day, n_orders=daily_orders)
    upload_to_s3([orders_path], cfg)

    return {
        "status": "ok",
        "run_dt": day.isoformat(),
        "rows_written": daily_orders,
        "s3_uri": f"s3://{bucket}/{s3_prefix}/orders/dt={day.isoformat()}/orders.csv",
    }
