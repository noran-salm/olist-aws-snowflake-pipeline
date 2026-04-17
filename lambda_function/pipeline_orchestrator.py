"""
lambda_function/pipeline_orchestrator.py — v2
Production hardened:
  - Structured JSON logging with correlation ID
  - Idempotent operations (safe to re-run)
  - Precise error typing for Step Functions retry decisions
"""
import boto3
import json
import logging
import os
import time
from datetime import datetime, timezone

# ── Structured Logger ─────────────────────────────────────────
class StructuredLogger:
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        self.run_id = ""

    def _log(self, level: int, event: str, **kwargs):
        record = {
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "level":      logging.getLevelName(level),
            "service":    "olist-orchestrator",
            "run_id":     self.run_id,
            "event":      event,
            **kwargs
        }
        self._logger.log(level, json.dumps(record))

    def info(self,  event, **kw): self._log(logging.INFO,  event, **kw)
    def warn(self,  event, **kw): self._log(logging.WARNING, event, **kw)
    def error(self, event, **kw): self._log(logging.ERROR, event, **kw)

log  = StructuredLogger("olist-orchestrator")
glue = boto3.client("glue")
s3   = boto3.client("s3")
cw   = boto3.client("cloudwatch")

CRAWLER = os.environ.get("GLUE_CRAWLER",  "olist-raw-crawler")
ETL_JOB = os.environ.get("GLUE_ETL_JOB", "olist-etl-job")
BUCKET  = os.environ.get("S3_BUCKET",     "olist-lake-516671521715")


def put_metric(name: str, value: float, unit: str = "Count"):
    """Publish custom CloudWatch metric."""
    try:
        cw.put_metric_data(
            Namespace="Olist/Pipeline",
            MetricData=[{
                "MetricName": name,
                "Value":      value,
                "Unit":       unit,
                "Dimensions": [{"Name": "Pipeline", "Value": "olist-etl"}]
            }]
        )
    except Exception:
        pass  # Never fail the pipeline on metric publishing


def start_crawler(event: dict) -> dict:
    """Idempotent: already-running crawlers are treated as success."""
    try:
        glue.start_crawler(Name=CRAWLER)
        log.info("crawler_started", crawler=CRAWLER)
        return {"status": "STARTED", "crawler": CRAWLER}
    except glue.exceptions.CrawlerRunningException:
        log.warn("crawler_already_running", crawler=CRAWLER)
        return {"status": "ALREADY_RUNNING", "crawler": CRAWLER}
    except glue.exceptions.EntityNotFoundException:
        raise RuntimeError(f"Crawler not found: {CRAWLER}") from None


def check_crawler(event: dict) -> dict:
    resp  = glue.get_crawler(Name=CRAWLER)
    state = resp["Crawler"]["State"]
    last  = resp["Crawler"].get("LastCrawl", {})
    log.info("crawler_status", state=state)

    if state == "READY":
        crawl_status = last.get("Status", "UNKNOWN")
        if crawl_status == "FAILED":
            raise RuntimeError(
                f"Crawler failed: {last.get('ErrorMessage', 'unknown')}"
            )
        tables_created = last.get("Summary", {}).get("tablesCreated", 0)
        tables_updated = last.get("Summary", {}).get("tablesUpdated", 0)
        put_metric("CrawlerTablesCreated", tables_created)
        put_metric("CrawlerTablesUpdated", tables_updated)
        return {"status": "READY", "crawl_status": crawl_status}
    return {"status": state}


def start_etl(event: dict) -> dict:
    """Idempotent: checks for already-running job before starting."""
    # Check if a run is already active (handles Step Functions re-runs)
    runs = glue.get_job_runs(JobName=ETL_JOB, MaxResults=1)["JobRuns"]
    if runs and runs[0]["JobRunState"] == "RUNNING":
        existing_run_id = runs[0]["Id"]
        log.warn("etl_already_running", job_run_id=existing_run_id)
        return {"status": "STARTED", "job_run_id": existing_run_id}

    run_id = glue.start_job_run(
        JobName=ETL_JOB,
        Arguments={
            "--SOURCE_BUCKET":       BUCKET,
            "--SOURCE_PREFIX":       "raw/",
            "--TARGET_PREFIX":       "processed/",
            "--DATABASE_NAME":       "olist_raw_db",
            "--job-bookmark-option": "job-bookmark-enable",
        }
    )["JobRunId"]

    log.info("etl_started", job_run_id=run_id)
    put_metric("ETLJobsStarted", 1)
    return {"status": "STARTED", "job_run_id": run_id}


def check_etl(event: dict) -> dict:
    run_id = event.get("job_run_id")
    if not run_id:
        raise ValueError("job_run_id missing from event")

    resp  = glue.get_job_run(JobName=ETL_JOB, RunId=run_id)
    state = resp["JobRun"]["JobRunState"]
    secs  = resp["JobRun"].get("ExecutionTime", 0)

    log.info("etl_status", state=state, duration_seconds=secs,
             job_run_id=run_id)

    if state == "SUCCEEDED":
        put_metric("ETLDurationSeconds", secs, "Seconds")
        put_metric("ETLJobsSucceeded",   1)
        return {"status": "SUCCEEDED", "job_run_id": run_id, "duration_s": secs}

    if state in ("FAILED", "STOPPED", "TIMEOUT"):
        error = resp["JobRun"].get("ErrorMessage", "Unknown")
        put_metric("ETLJobsFailed", 1)
        raise RuntimeError(f"Glue ETL {state} after {secs}s: {error}")

    return {"status": state, "job_run_id": run_id, "duration_s": secs}


def validate_s3(event: dict) -> dict:
    """Validates processed/ Parquet output exists."""
    prefixes = [
        "processed/dim_customers/",
        "processed/dim_products/",
        "processed/dim_sellers/",
        "processed/fct_orders/",
    ]
    total_files = 0
    for prefix in prefixes:
        resp  = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        files = [o for o in resp.get("Contents", [])
                 if o["Key"].endswith(".parquet")]
        if not files:
            raise RuntimeError(
                f"No Parquet files in s3://{BUCKET}/{prefix} — ETL may have failed silently"
            )
        total_files += len(files)
        log.info("s3_validated", prefix=prefix, file_count=len(files))

    put_metric("ProcessedParquetFiles", total_files)
    return {"status": "VALID", "total_files": total_files}


ACTIONS = {
    "start_crawler": start_crawler,
    "check_crawler": check_crawler,
    "start_etl":     start_etl,
    "check_etl":     check_etl,
    "validate_s3":   validate_s3,
}

def handler(event, context):
    log.run_id = event.get("run_id",
        datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
    action = event.get("action")
    log.info("action_started", action=action, event=event)

    if action not in ACTIONS:
        raise ValueError(f"Unknown action '{action}'. Valid: {list(ACTIONS)}")

    t0     = time.time()
    result = ACTIONS[action](event)
    ms     = int((time.time() - t0) * 1000)

    put_metric(f"ActionDuration_{action}", ms, "Milliseconds")
    log.info("action_completed", action=action,
             result=result, duration_ms=ms)
    return result
