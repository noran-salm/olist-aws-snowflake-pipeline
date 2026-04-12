import json
import logging
import os
import time
from typing import Any
import boto3
from botocore.exceptions import ClientError

LOG_LEVEL    = os.environ.get("LOG_LEVEL", "INFO").upper()
S3_BUCKET    = os.environ["S3_BUCKET"]
S3_PREFIX    = os.environ.get("S3_RAW_PREFIX", "raw/").rstrip("/") + "/"
GLUE_CRAWLER = os.environ.get("GLUE_CRAWLER", "olist-raw-crawler")
GLUE_JOB     = os.environ.get("GLUE_ETL_JOB", "olist-etl-job")

EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    level=getattr(logging, LOG_LEVEL, logging.INFO),
)
logger = logging.getLogger("olist-ingestion")

s3   = boto3.client("s3")
glue = boto3.client("glue")

def list_raw_files() -> set:
    paginator = s3.get_paginator("list_objects_v2")
    keys = set()
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                keys.add(key.split("/")[-1])
    return keys

def validate_files() -> dict:
    present  = list_raw_files()
    expected = set(EXPECTED_FILES)
    missing  = expected - present
    logger.info("S3 raw/ has %d CSVs. Expected: %d.", len(present), len(expected))
    if missing:
        logger.warning("Missing: %s", missing)
    return {
        "present":     sorted(present),
        "missing":     sorted(missing),
        "all_present": len(missing) == 0,
    }

def start_crawler() -> str:
    try:
        glue.start_crawler(Name=GLUE_CRAWLER)
        logger.info("Crawler '%s' started.", GLUE_CRAWLER)
        return "STARTED"
    except glue.exceptions.CrawlerRunningException:
        logger.warning("Crawler already running.")
        return "ALREADY_RUNNING"

def start_etl_job() -> str:
    resp = glue.start_job_run(
        JobName=GLUE_JOB,
        Arguments={
            "--SOURCE_BUCKET": S3_BUCKET,
            "--SOURCE_PREFIX": S3_PREFIX,
            "--TARGET_PREFIX": "processed/",
            "--DATABASE_NAME": "olist_raw_db",
        },
    )
    run_id = resp["JobRunId"]
    logger.info("ETL job started. RunId: %s", run_id)
    return run_id

def wait_for_crawler(max_wait: int = 600) -> str:
    elapsed, interval = 0, 20
    while elapsed < max_wait:
        resp  = glue.get_crawler(Name=GLUE_CRAWLER)
        state = resp["Crawler"]["State"]
        logger.info("Crawler state: %s (%ds)", state, elapsed)
        if state == "READY":
            return resp["Crawler"].get("LastCrawl", {}).get("Status", "SUCCEEDED")
        time.sleep(interval)
        elapsed += interval
    return "TIMEOUT"

def handler(event: dict, context: Any) -> dict:
    logger.info("Event: %s", json.dumps(event))

    skip_crawler = event.get("skip_crawler", False)
    start_etl    = event.get("start_etl", False)
    wait_crawler = event.get("wait_for_crawler", False)
    result       = {"status": "success"}

    try:
        file_status = validate_files()
        result["file_status"] = file_status

        if not file_status["all_present"]:
            result["status"]  = "warning"
            result["message"] = (
                f"Missing {len(file_status['missing'])} file(s): "
                f"{file_status['missing']}. Upload them to "
                f"s3://{S3_BUCKET}/{S3_PREFIX} first."
            )
            logger.warning(result["message"])
            return result

        if not skip_crawler:
            result["crawler_status"] = start_crawler()
            if wait_crawler:
                result["crawler_final_status"] = wait_for_crawler()

        if start_etl:
            result["etl_job_run_id"] = start_etl_job()

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        result["status"] = "error"
        result["error"]  = str(exc)
        raise

    logger.info("Result: %s", json.dumps(result, default=str))
    return result
