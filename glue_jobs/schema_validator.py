"""
glue_jobs/schema_validator.py
Runs as a Glue Python Shell job (cheap — no Spark).
Detects schema changes vs last known schema.
Stores fingerprints in S3 for comparison.
Fails loudly if breaking changes detected.
"""
import boto3
import json
import hashlib
import sys
from datetime import datetime, timezone

s3 = boto3.client("s3")

BUCKET           = "olist-lake-516671521715"
SCHEMA_PREFIX    = "schema-registry/"
RAW_PREFIX       = "raw/"

# Canonical schema — source of truth
EXPECTED_SCHEMAS = {
    "olist_orders_dataset.csv": {
        "order_id":                    "string",
        "customer_id":                 "string",
        "order_status":                "string",
        "order_purchase_timestamp":    "string",
        "order_approved_at":           "string",
        "order_delivered_carrier_date":"string",
        "order_delivered_customer_date":"string",
        "order_estimated_delivery_date":"string",
    },
    "olist_order_items_dataset.csv": {
        "order_id":         "string",
        "order_item_id":    "string",
        "product_id":       "string",
        "seller_id":        "string",
        "shipping_limit_date":"string",
        "price":            "string",
        "freight_value":    "string",
    },
    "olist_customers_dataset.csv": {
        "customer_id":              "string",
        "customer_unique_id":       "string",
        "customer_zip_code_prefix": "string",
        "customer_city":            "string",
        "customer_state":           "string",
    },
}


def get_csv_headers(filename: str) -> list:
    """Read only first line of CSV to get column names."""
    import csv, io
    obj = s3.get_object(Bucket=BUCKET, Key=f"{RAW_PREFIX}{filename}")
    first_line = obj["Body"].read(8192).decode("utf-8", errors="replace")\
                    .split("\n")[0]
    reader = csv.reader(io.StringIO(first_line))
    return [c.strip().strip('"').lower() for c in next(reader)]


def fingerprint(schema: dict) -> str:
    return hashlib.md5(
        json.dumps(schema, sort_keys=True).encode()
    ).hexdigest()


def load_last_schema(filename: str) -> dict | None:
    key = f"{SCHEMA_PREFIX}{filename}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def save_schema(filename: str, schema: dict):
    key = f"{SCHEMA_PREFIX}{filename}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps({
            "schema":      schema,
            "fingerprint": fingerprint(schema),
            "saved_at":    datetime.now(timezone.utc).isoformat(),
        }, indent=2).encode(),
        ContentType="application/json"
    )


def validate_schema(filename: str) -> dict:
    current_cols = get_csv_headers(filename)
    current_schema = {col: "string" for col in current_cols}
    expected = EXPECTED_SCHEMAS.get(filename)
    last     = load_last_schema(filename)

    issues        = []
    change_type   = "none"

    if expected:
        # Check for missing required columns (BREAKING)
        missing = [c for c in expected if c not in current_schema]
        if missing:
            issues.append({
                "severity": "CRITICAL",
                "type":     "missing_columns",
                "columns":  missing,
                "message":  f"Required columns missing: {missing}"
            })
            change_type = "breaking"

        # Check for new columns (NON-BREAKING — log only)
        new_cols = [c for c in current_schema if c not in expected]
        if new_cols:
            issues.append({
                "severity": "INFO",
                "type":     "new_columns",
                "columns":  new_cols,
                "message":  f"New columns detected (safe): {new_cols}"
            })
            if change_type != "breaking":
                change_type = "additive"

    # Compare with last known schema
    if last and last.get("fingerprint") != fingerprint(current_schema):
        if change_type not in ("breaking",):
            change_type = "changed"

    save_schema(filename, current_schema)
    return {
        "filename":       filename,
        "change_type":    change_type,
        "current_cols":   len(current_cols),
        "issues":         issues,
        "fingerprint":    fingerprint(current_schema),
    }


def main():
    print(f"[{datetime.now().isoformat()}] Starting schema validation...")
    results = []
    critical_failures = []

    for filename in EXPECTED_SCHEMAS:
        result = validate_schema(filename)
        results.append(result)
        print(json.dumps(result, indent=2))

        if any(i["severity"] == "CRITICAL" for i in result["issues"]):
            critical_failures.append(filename)

    # Save validation report to S3
    report = {
        "validated_at":      datetime.now(timezone.utc).isoformat(),
        "total_files":       len(results),
        "critical_failures": critical_failures,
        "results":           results,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=f"schema-registry/validation-report-latest.json",
        Body=json.dumps(report, indent=2).encode(),
    )

    if critical_failures:
        print(f"CRITICAL: Schema breaking changes in: {critical_failures}")
        sys.exit(1)   # Non-zero exit fails the Glue Python Shell job

    print(f"Schema validation PASSED for {len(results)} files")


if __name__ == "__main__":
    main()
