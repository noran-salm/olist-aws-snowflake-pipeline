"""
lambda_function/pipeline_orchestrator.py
Called by Step Functions at each pipeline stage.
Action is passed in the event dict.
"""
import boto3
import json
import logging
import os
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue   = boto3.client("glue")
s3     = boto3.client("s3")

CRAWLER  = os.environ.get("GLUE_CRAWLER",  "olist-raw-crawler")
ETL_JOB  = os.environ.get("GLUE_ETL_JOB", "olist-etl-job")
BUCKET   = os.environ.get("S3_BUCKET",     "olist-lake-516671521715")


def start_crawler(event):
    try:
        glue.start_crawler(Name=CRAWLER)
        logger.info("Crawler started: %s", CRAWLER)
        return {"status": "STARTED", "crawler": CRAWLER}
    except glue.exceptions.CrawlerRunningException:
        logger.warning("Crawler already running")
        return {"status": "ALREADY_RUNNING", "crawler": CRAWLER}


def check_crawler(event):
    resp  = glue.get_crawler(Name=CRAWLER)
    state = resp["Crawler"]["State"]
    last  = resp["Crawler"].get("LastCrawl", {})

    logger.info("Crawler state: %s", state)

    if state == "READY":
        status = last.get("Status", "UNKNOWN")
        if status == "FAILED":
            raise Exception(f"Crawler failed: {last.get('ErrorMessage','unknown error')}")
        return {"status": "READY", "crawl_status": status}

    return {"status": state}  # RUNNING or STOPPING


def start_etl(event):
    run_id = glue.start_job_run(
        JobName   = ETL_JOB,
        Arguments = {
            "--SOURCE_BUCKET":        BUCKET,
            "--SOURCE_PREFIX":        "raw/",
            "--TARGET_PREFIX":        "processed/",
            "--DATABASE_NAME":        "olist_raw_db",
            "--job-bookmark-option":  "job-bookmark-enable",
        }
    )["JobRunId"]

    logger.info("ETL job started: %s", run_id)
    return {"status": "STARTED", "job_run_id": run_id}


def check_etl(event):
    run_id = event.get("job_run_id")
    if not run_id:
        raise ValueError("job_run_id missing from event")

    resp  = glue.get_job_run(JobName=ETL_JOB, RunId=run_id)
    state = resp["JobRun"]["JobRunState"]
    secs  = resp["JobRun"].get("ExecutionTime", 0)

    logger.info("ETL state: %s (%ds)", state, secs)

    if state == "FAILED":
        error = resp["JobRun"].get("ErrorMessage", "Unknown error")
        raise Exception(f"Glue ETL failed after {secs}s: {error}")

    if state in ("STOPPING", "STOPPED"):
        raise Exception(f"Glue ETL was stopped unexpectedly")

    return {
        "status":      state,
        "job_run_id":  run_id,
        "duration_s":  secs,
    }


def validate_s3(event):
    """Quick sanity check that processed/ files exist."""
    prefixes = [
        "processed/dim_customers/",
        "processed/dim_products/",
        "processed/dim_sellers/",
        "processed/fct_orders/",
    ]
    for prefix in prefixes:
        resp  = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        files = [o for o in resp.get("Contents", [])
                 if o["Key"].endswith(".parquet")]
        if not files:
            raise Exception(f"No Parquet files in s3://{BUCKET}/{prefix}")
        logger.info("✅ %s: %d files", prefix, len(files))

    return {"status": "VALID", "message": "All S3 outputs present"}


# ── Router ─────────────────────────────────────────────────────
ACTIONS = {
    "start_crawler": start_crawler,
    "check_crawler": check_crawler,
    "start_etl":     start_etl,
    "check_etl":     check_etl,
    "validate_s3":   validate_s3,
}

def handler(event, context):
    action = event.get("action")
    logger.info("Action: %s | Event: %s", action, json.dumps(event))

    if action not in ACTIONS:
        raise ValueError(f"Unknown action: {action}. Valid: {list(ACTIONS)}")

    result = ACTIONS[action](event)
    logger.info("Result: %s", json.dumps(result, default=str))
    return result
