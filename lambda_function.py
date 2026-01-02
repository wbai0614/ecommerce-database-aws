# lambda_function.py (UPDATED)
# Pipeline:
# 1) Generate daily raw orders CSV -> upload to S3 (raw/orders/dt=YYYY-MM-DD/orders.csv)
# 2) Start Glue ETL with required args (including RAW_BUCKET/RAW_PREFIX) -> wait until SUCCEEDED
# 3) Call Redshift stored procedure: CALL public.sp_load_orders_daily('<RUN_DT>')

import csv
import os
import random
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import List

import boto3

random.seed(42)

PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "APPLE_PAY", "GOOGLE_PAY", "BANK_TRANSFER"]
ORDER_STATUS = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "REFUNDED"]  # weighted


@dataclass
class Config:
    out_dir: str
    n_customers: int = 10_000
    n_products: int = 500
    s3_bucket: str = ""
    s3_prefix: str = "raw"


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
    customer_ids = [f"C{i:06d}" for i in range(1, cfg.n_customers + 1)]
    product_ids = [f"P{i:06d}" for i in range(1, cfg.n_products + 1)]

    rows: List[List[str]] = []
    for i in range(1, n_orders + 1):
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
    s3 = boto3.client("s3")
    for p in paths:
        rel = p.relative_to(Path(cfg.out_dir))
        key = f"{cfg.s3_prefix}/{rel.as_posix()}"
        print(f"Uploading {p} -> s3://{cfg.s3_bucket}/{key}")
        s3.upload_file(str(p), cfg.s3_bucket, key)


def start_glue_job(run_dt: str, raw_bucket: str, raw_prefix: str) -> str:
    glue = boto3.client("glue")
    job_name = os.environ["GLUE_JOB_NAME"]
    raw_db = os.environ["RAW_DATABASE"]
    s3_output_base = os.environ["S3_OUTPUT_BASE"]

    # IMPORTANT: pass RAW_BUCKET + RAW_PREFIX because your Glue script requires them
    resp = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--RUN_DT": run_dt,
            "--RAW_DATABASE": raw_db,
            "--S3_OUTPUT_BASE": s3_output_base,
            "--RAW_BUCKET": raw_bucket,
            "--RAW_PREFIX": raw_prefix,
        },
    )
    return resp["JobRunId"]


def wait_for_glue(job_run_id: str) -> None:
    glue = boto3.client("glue")
    job_name = os.environ["GLUE_JOB_NAME"]

    timeout_sec = int(os.environ.get("GLUE_WAIT_TIMEOUT_SEC", "900"))
    poll_sec = int(os.environ.get("GLUE_POLL_SEC", "15"))

    start = time.time()
    while True:
        jr = glue.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)["JobRun"]
        state = jr["JobRunState"]
        print(f"Glue status: {state}")

        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "TIMEOUT", "STOPPED", "ERROR"):
            raise RuntimeError(f"Glue job ended in state={state}. ErrorMessage={jr.get('ErrorMessage')}")
        if time.time() - start > timeout_sec:
            raise TimeoutError("Timed out waiting for Glue job to finish.")
        time.sleep(poll_sec)


def redshift_execute(sql: str) -> str:
    rsd = boto3.client("redshift-data")
    workgroup = os.environ["REDSHIFT_WORKGROUP"]
    database = os.environ.get("REDSHIFT_DATABASE", "dev")
    secret_arn = os.environ["REDSHIFT_SECRET_ARN"]

    resp = rsd.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql,
    )
    return resp["Id"]


def wait_for_redshift(statement_id: str) -> None:
    rsd = boto3.client("redshift-data")

    timeout_sec = int(os.environ.get("RS_WAIT_TIMEOUT_SEC", "600"))
    poll_sec = int(os.environ.get("RS_POLL_SEC", "2"))

    start = time.time()
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        status = desc["Status"]
        if status == "FINISHED":
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift statement {status}: {desc.get('Error')}")
        if time.time() - start > timeout_sec:
            raise TimeoutError("Timed out waiting for Redshift statement.")
        time.sleep(poll_sec)


def load_redshift_for_dt(run_dt: str) -> None:
    schema = os.environ.get("REDSHIFT_PROC_SCHEMA", "public")
    proc = os.environ.get("REDSHIFT_PROC_NAME", "sp_load_orders_daily")
    sql = f"CALL {schema}.{proc}('{run_dt}');"
    sid = redshift_execute(sql)
    wait_for_redshift(sid)


def lambda_handler(event, context):
    run_dt = (event or {}).get("run_dt")
    day = date.fromisoformat(run_dt) if run_dt else date.today()

    bucket = os.environ["BUCKET_NAME"]
    daily_orders = int(os.environ.get("DAILY_ORDERS", "1000"))
    s3_prefix = os.environ.get("S3_PREFIX", "raw").strip("/")

    cfg = Config(
        out_dir="/tmp/synthetic_out",
        s3_bucket=bucket,
        s3_prefix=s3_prefix,
    )

    print(f"Generating {daily_orders} orders for dt={day.isoformat()} ...")
    orders_path = generate_orders_for_date(cfg, day, n_orders=daily_orders)
    upload_to_s3([orders_path], cfg)

    raw_s3_uri = f"s3://{bucket}/{s3_prefix}/orders/dt={day.isoformat()}/orders.csv"

    print("Starting Glue job...")
    jr_id = start_glue_job(day.isoformat(), raw_bucket=bucket, raw_prefix=s3_prefix)
    print(f"Glue JobRunId: {jr_id}")
    wait_for_glue(jr_id)

    print("Calling Redshift procedure public.sp_load_orders_daily ...")
    load_redshift_for_dt(day.isoformat())

    curated_s3_uri = f"s3://{bucket}/curated/orders/dt={day.isoformat()}/"

    return {
        "status": "ok",
        "run_dt": day.isoformat(),
        "rows_written": daily_orders,
        "raw_s3_uri": raw_s3_uri,
        "glue_job_run_id": jr_id,
        "curated_s3_uri": curated_s3_uri,
        "redshift_proc_called": "public.sp_load_orders_daily",
        "redshift_loaded": True,
    }
