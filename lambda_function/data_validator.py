"""
lambda_function/data_validator.py
Runs BEFORE Glue ETL — validates raw CSVs in S3.
Called by Step Functions as "ValidateRawData" step.
Writes results to Snowflake AUDIT.pipeline_validation_log.
"""
import boto3
import json
import logging
import os
import time
import csv
import io
from datetime import datetime, timezone, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3      = boto3.client("s3")
sns     = boto3.client("sns")

BUCKET    = os.environ.get("S3_BUCKET",   "olist-lake-516671521715")
SNS_ARN   = os.environ.get("SNS_ARN",     "")
RAW_PREFIX = "raw/"

# Expected files + minimum row counts
EXPECTED_FILES = {
    "olist_orders_dataset.csv":          50000,
    "olist_order_items_dataset.csv":     100000,
    "olist_customers_dataset.csv":       90000,
    "olist_products_dataset.csv":        30000,
    "olist_sellers_dataset.csv":         2000,
    "olist_order_payments_dataset.csv":  100000,
    "olist_order_reviews_dataset.csv":   90000,
    "product_category_name_translation.csv": 70,
}

# Required columns per file
REQUIRED_COLUMNS = {
    "olist_orders_dataset.csv": [
        "order_id","customer_id","order_status","order_purchase_timestamp"
    ],
    "olist_order_items_dataset.csv": [
        "order_id","order_item_id","product_id","seller_id","price","freight_value"
    ],
    "olist_customers_dataset.csv": [
        "customer_id","customer_unique_id","customer_zip_code_prefix",
        "customer_city","customer_state"
    ],
}


def log_check(run_id, check_name, stage, status,
              rows_checked=0, rows_failed=0, error_msg="", duration_ms=0):
    """Structured log — also returned for Step Functions state."""
    result = {
        "run_id":       run_id,
        "check_name":   check_name,
        "check_stage":  stage,
        "status":       status,
        "rows_checked": rows_checked,
        "rows_failed":  rows_failed,
        "error_message":error_msg,
        "duration_ms":  duration_ms,
    }
    level = logging.ERROR if status == "FAIL" else logging.INFO
    logger.log(level, json.dumps({"validation": result}))
    return result


def check_file_exists(run_id: str) -> list:
    results = []
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)
    existing = {
        o["Key"].split("/")[-1]
        for o in resp.get("Contents", [])
        if o["Key"].endswith(".csv")
    }

    for filename in EXPECTED_FILES:
        status = "PASS" if filename in existing else "FAIL"
        results.append(log_check(
            run_id, f"file_exists_{filename}", "S3", status,
            error_msg="" if status == "PASS" else f"Missing: {filename}"
        ))
    return results


def check_file_freshness(run_id: str, max_age_hours: int = 48) -> list:
    results = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)

    for obj in resp.get("Contents", []):
        fname = obj["Key"].split("/")[-1]
        if not fname.endswith(".csv"):
            continue
        age_hrs = (datetime.now(timezone.utc) -
                   obj["LastModified"]).total_seconds() / 3600
        status = "PASS" if obj["LastModified"] >= cutoff else "WARN"
        results.append(log_check(
            run_id, f"freshness_{fname}", "S3", status,
            error_msg=f"File is {age_hrs:.1f}h old" if status == "WARN" else ""
        ))
    return results


def check_row_counts(run_id: str) -> list:
    results = []
    for filename, min_rows in EXPECTED_FILES.items():
        t0 = time.time()
        try:
            obj = s3.get_object(
                Bucket=BUCKET, Key=f"{RAW_PREFIX}{filename}"
            )
            content = obj["Body"].read().decode("utf-8", errors="replace")
            row_count = content.count("\n") - 1  # subtract header
            status = "PASS" if row_count >= min_rows else "FAIL"
            results.append(log_check(
                run_id, f"row_count_{filename}", "S3", status,
                rows_checked=row_count,
                rows_failed=0 if status == "PASS" else min_rows - row_count,
                error_msg="" if status == "PASS" else
                    f"Only {row_count:,} rows, expected >= {min_rows:,}",
                duration_ms=int((time.time()-t0)*1000)
            ))
        except Exception as e:
            results.append(log_check(
                run_id, f"row_count_{filename}", "S3", "FAIL",
                error_msg=str(e)
            ))
    return results


def check_required_columns(run_id: str) -> list:
    results = []
    for filename, required_cols in REQUIRED_COLUMNS.items():
        t0 = time.time()
        try:
            obj = s3.get_object(
                Bucket=BUCKET, Key=f"{RAW_PREFIX}{filename}"
            )
            # Read only first line for headers
            first_line = obj["Body"].read(4096).decode(
                "utf-8", errors="replace"
            ).split("\n")[0]
            reader = csv.reader(io.StringIO(first_line))
            actual_cols = [c.strip().strip('"') for c in next(reader)]
            missing = [c for c in required_cols if c not in actual_cols]
            status = "PASS" if not missing else "FAIL"
            results.append(log_check(
                run_id, f"schema_{filename}", "S3", status,
                rows_checked=len(actual_cols),
                rows_failed=len(missing),
                error_msg="" if status == "PASS" else
                    f"Missing columns: {missing}",
                duration_ms=int((time.time()-t0)*1000)
            ))
        except Exception as e:
            results.append(log_check(
                run_id, f"schema_{filename}", "S3", "FAIL",
                error_msg=str(e)
            ))
    return results


def handler(event, context):
    run_id = event.get("run_id",
        datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))

    logger.info(json.dumps({
        "event": "validation_started",
        "run_id": run_id,
        "stage": "S3_RAW"
    }))

    all_results = []
    all_results += check_file_exists(run_id)
    all_results += check_file_freshness(run_id)
    all_results += check_row_counts(run_id)
    all_results += check_required_columns(run_id)

    failures = [r for r in all_results if r["status"] == "FAIL"]
    warnings = [r for r in all_results if r["status"] == "WARN"]

    summary = {
        "run_id":         run_id,
        "total_checks":   len(all_results),
        "passed":         len(all_results) - len(failures) - len(warnings),
        "failed":         len(failures),
        "warnings":       len(warnings),
        "validation_results": all_results,
    }

    if failures:
        # Send SNS alert for failures
        if SNS_ARN:
            sns.publish(
                TopicArn=SNS_ARN,
                Subject=f"🚨 Olist Data Quality FAILED — {len(failures)} checks",
                Message=json.dumps({
                    "run_id": run_id,
                    "failures": failures
                }, indent=2)
            )
        # Raise exception — Step Functions catches this and routes to failure
        raise Exception(
            f"Data validation FAILED: {len(failures)} critical check(s). "
            f"Failures: {[f['check_name'] for f in failures]}"
        )

    logger.info(json.dumps({"event": "validation_passed", **summary}))
    return summary
