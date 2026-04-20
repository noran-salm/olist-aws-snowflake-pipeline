"""
lambda_function/health_check.py
Pipeline health check — runs after Glue ETL completes.
Validates:
  1. S3 processed files updated in last 24h
  2. Snowflake RAW table row counts within expected ranges
  3. No NULL order_year_month in fct_orders
Sends SNS alert if any check fails.
"""
import boto3
import json
import logging
import os
from datetime import datetime, timezone, timedelta
import snowflake.connector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ─────────────────────────────────────────────────────
BUCKET      = os.environ.get("S3_BUCKET",   "olist-lake-516671521715")
SNS_ARN     = os.environ.get("SNS_ARN",     "")
SECRET_NAME = os.environ.get("SECRET_NAME", "olist/snowflake/credentials")
REGION      = os.environ.get("AWS_REGION",  "us-east-1")

# Expected row count ranges (min, max)
EXPECTED_COUNTS = {
    "dim_customers": (90_000,  120_000),
    "dim_products":  (30_000,   40_000),
    "dim_sellers":   ( 2_000,    5_000),
    "fct_orders":    (100_000, 130_000),
}

# ── AWS Clients ─────────────────────────────────────────────────
s3     = boto3.client("s3")
sns    = boto3.client("sns",              region_name=REGION)
secret = boto3.client("secretsmanager",  region_name=REGION)


def get_snowflake_creds() -> dict:
    resp = secret.get_secret_value(SecretId=SECRET_NAME)
    return json.loads(resp["SecretString"])


def get_snowflake_conn():
    creds = get_snowflake_creds()
    return snowflake.connector.connect(
        account   = creds["account"],
        user      = creds["user"],
        password  = creds["password"],
        role      = creds.get("role",      "SYSADMIN"),
        database  = creds.get("database",  "OLIST_DW"),
        warehouse = creds.get("warehouse", "OLIST_WH"),
        schema    = "RAW",
    )


def check_s3_freshness() -> list[str]:
    """Check processed/ files were updated in last 24 hours."""
    issues = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    prefixes = ["processed/dim_customers/", "processed/dim_products/",
                "processed/dim_sellers/",   "processed/fct_orders/"]

    for prefix in prefixes:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        if not objects:
            issues.append(f"❌ S3: No files in {prefix}")
            continue
        latest = max(o["LastModified"] for o in objects)
        if latest < cutoff:
            age_hrs = (datetime.now(timezone.utc) - latest).seconds // 3600
            issues.append(
                f"⚠️  S3: {prefix} last updated {age_hrs}h ago (> 24h threshold)"
            )
        else:
            logger.info("✅ S3 fresh: %s (updated %s)", prefix, latest)
    return issues


def check_snowflake_counts() -> list[str]:
    """Validate row counts in Snowflake RAW tables."""
    issues = []
    try:
        conn = get_snowflake_conn()
        cur  = conn.cursor()

        for table, (min_rows, max_rows) in EXPECTED_COUNTS.items():
            cur.execute(f"SELECT COUNT(*) FROM OLIST_DW.RAW.{table}")
            count = cur.fetchone()[0]
            if count < min_rows:
                issues.append(
                    f"❌ Snowflake: {table} has {count:,} rows "
                    f"(expected >= {min_rows:,})"
                )
            elif count > max_rows:
                issues.append(
                    f"⚠️  Snowflake: {table} has {count:,} rows "
                    f"(expected <= {max_rows:,} — possible duplicate load)"
                )
            else:
                logger.info("✅ %s: %s rows (within range)", table, f"{count:,}")

        # Check for NULL order_year_month
        cur.execute("""
            SELECT COUNT(*) FROM OLIST_DW.RAW.fct_orders
            WHERE order_year_month IS NULL
        """)
        null_count = cur.fetchone()[0]
        if null_count > 0:
            issues.append(
                f"❌ Snowflake: fct_orders has {null_count:,} rows "
                f"with NULL order_year_month"
            )

        conn.close()
    except Exception as e:
        issues.append(f"❌ Snowflake connection failed: {str(e)}")

    return issues


def send_alert(issues: list[str]) -> None:
    """Send SNS alert with all issues found."""
    if not SNS_ARN:
        logger.warning("SNS_ARN not set — skipping alert")
        return

    message = (
        "🚨 OLIST PIPELINE HEALTH CHECK FAILED\n"
        f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Issues found: {len(issues)}\n\n"
        + "\n".join(issues)
        + "\n\nCheck CloudWatch logs for details."
    )

    sns.publish(
        TopicArn = SNS_ARN,
        Subject  = f"🚨 Olist Pipeline Alert — {len(issues)} issue(s) detected",
        Message  = message,
    )
    logger.error("Alert sent: %d issues", len(issues))


def handler(event: dict, context) -> dict:
    logger.info("Starting pipeline health check...")
    all_issues = []

    # Run all checks
    all_issues += check_s3_freshness()
    all_issues += check_snowflake_counts()

    if all_issues:
        send_alert(all_issues)
        return {
            "status":      "UNHEALTHY",
            "issues_found": len(all_issues),
            "issues":       all_issues,
        }

    logger.info("✅ All health checks passed")
    return {
        "status":      "HEALTHY",
        "issues_found": 0,
        "message":      "All pipeline checks passed",
    }
