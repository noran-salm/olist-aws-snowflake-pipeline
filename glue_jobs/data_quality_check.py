"""
data_quality_check.py
Run after Glue ETL to validate processed Parquet files before Snowflake load.
Usage: python glue_jobs/data_quality_check.py
"""
import boto3
import pandas as pd
import pyarrow.parquet as pq
import io
import sys

BUCKET = "olist-lake-516671521715"
CHECKS_PASSED = True

s3 = boto3.client("s3")

def check_table(prefix: str, checks: list):
    global CHECKS_PASSED
    print(f"\n=== Checking {prefix} ===")

    # Get first Parquet file
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"processed/{prefix}/")
    files = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

    if not files:
        print(f"  ❌ No Parquet files found in processed/{prefix}/")
        CHECKS_PASSED = False
        return

    obj = s3.get_object(Bucket=BUCKET, Key=files[0])
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    print(f"  Rows: {len(df):,} | Cols: {list(df.columns)}")

    for col, check_type, *args in checks:
        if check_type == "not_null":
            nulls = df[col].isnull().sum()
            status = "✅" if nulls == 0 else "❌"
            print(f"  {status} {col} not_null (nulls={nulls})")
            if nulls > 0: CHECKS_PASSED = False

        elif check_type == "between":
            lo, hi = args
            bad = ((df[col] < lo) | (df[col] > hi)).sum()
            status = "✅" if bad == 0 else "⚠️"
            print(f"  {status} {col} between {lo}-{hi} (violations={bad})")

        elif check_type == "unique":
            dups = df[col].duplicated().sum()
            status = "✅" if dups == 0 else "❌"
            print(f"  {status} {col} unique (duplicates={dups})")
            if dups > 0: CHECKS_PASSED = False

        elif check_type == "accepted_values":
            allowed = set(args[0])
            bad_vals = (~df[col].isin(allowed)).sum()
            status = "✅" if bad_vals == 0 else "⚠️"
            print(f"  {status} {col} accepted_values (violations={bad_vals})")

# Run checks on each table
check_table("dim_customers", [
    ("customer_id",        "not_null"),
    ("customer_id",        "unique"),
    ("customer_unique_id", "not_null"),
    ("state",              "not_null"),
])

check_table("dim_products", [
    ("product_id", "not_null"),
    ("product_id", "unique"),
    ("category",   "not_null"),
])

check_table("dim_sellers", [
    ("seller_id", "not_null"),
    ("seller_id", "unique"),
    ("state",     "not_null"),
])

check_table("fct_orders", [
    ("order_id",           "not_null"),
    ("customer_id",        "not_null"),
    ("price",              "not_null"),
    ("price",              "between", 0, 15000),
    ("order_item_revenue", "not_null"),
    ("order_status",       "accepted_values",
        ["delivered","shipped","canceled","processing",
         "invoiced","unavailable","approved","created"]),
])

print("\n" + "="*40)
if CHECKS_PASSED:
    print("✅ All quality checks PASSED")
    sys.exit(0)
else:
    print("❌ Some checks FAILED — review before loading to Snowflake")
    sys.exit(1)
