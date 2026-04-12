"""
olist_etl_script.py — Reads directly from S3 (bypasses catalog column name issue)
All Glue-crawled tables had unnamed columns (col0, col1...) because the crawler
did not detect headers. Reading with spark.read.csv header=True fixes this cleanly.
"""
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "SOURCE_BUCKET", "SOURCE_PREFIX", "TARGET_PREFIX", "DATABASE_NAME"
])

sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

BUCKET   = args["SOURCE_BUCKET"]
RAW      = f"s3://{BUCKET}/{args['SOURCE_PREFIX']}"
OUT_BASE = f"s3://{BUCKET}/{args['TARGET_PREFIX']}"

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


# ── Read directly from S3 with proper headers ─────────────────────────────────

def read_csv(filename: str) -> DataFrame:
    path = f"{RAW}{filename}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "false")   # keep everything as string first
          .option("quote", '"')
          .option("escape", '"')
          .option("multiLine", "true")
          .csv(path))
    print(f"[INFO] {filename}: {df.count()} rows | cols: {df.columns}")
    return df

def clean(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates()
    for col, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col, F.trim(F.col(col)))
    return df

def write_parquet(df: DataFrame, name: str, partition_cols=None):
    path = f"{OUT_BASE}{name}/"
    w = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.save(path)
    print(f"[INFO] Written → {path}")


# ── Load all source CSVs ──────────────────────────────────────────────────────

print("[INFO] Loading CSVs from S3...")
orders_df      = clean(read_csv("olist_orders_dataset.csv"))
order_items_df = clean(read_csv("olist_order_items_dataset.csv"))
customers_df   = clean(read_csv("olist_customers_dataset.csv"))
products_df    = clean(read_csv("olist_products_dataset.csv"))
sellers_df     = clean(read_csv("olist_sellers_dataset.csv"))
payments_df    = clean(read_csv("olist_order_payments_dataset.csv"))
reviews_df     = clean(read_csv("olist_order_reviews_dataset.csv"))
category_df    = clean(read_csv("product_category_name_translation.csv"))

# Sanity-check columns loaded correctly
print(f"[CHECK] orders cols:      {orders_df.columns}")
print(f"[CHECK] order_items cols: {order_items_df.columns}")
print(f"[CHECK] category cols:    {category_df.columns}")


# ── dim_customers ─────────────────────────────────────────────────────────────

print("[INFO] Building dim_customers...")
dim_customers = (
    customers_df
    .select(
        F.col("customer_id"),
        F.col("customer_unique_id"),
        F.col("customer_zip_code_prefix").cast("string").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )
    .dropDuplicates(["customer_id"])
    .withColumn("dbt_updated_at", F.current_timestamp())
)
write_parquet(dim_customers, "dim_customers")


# ── dim_products ──────────────────────────────────────────────────────────────

print("[INFO] Building dim_products...")
dim_products = (
    products_df
    .join(category_df, on="product_category_name", how="left")
    .withColumn(
        "category",
        F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name"),
            F.lit("unknown")
        )
    )
    .withColumn(
        "product_weight_kg",
        F.round(F.col("product_weight_g").cast(DoubleType()) / 1000, 3)
    )
    .select(
        F.col("product_id"),
        F.col("category"),
        F.col("product_name_lenght").cast(IntegerType()).alias("name_length"),
        F.col("product_description_lenght").cast(IntegerType()).alias("description_length"),
        F.col("product_photos_qty").cast(IntegerType()).alias("photos_qty"),
        F.col("product_weight_kg"),
        F.col("product_length_cm").cast(DoubleType()).alias("length_cm"),
        F.col("product_height_cm").cast(DoubleType()).alias("height_cm"),
        F.col("product_width_cm").cast(DoubleType()).alias("width_cm"),
    )
    .dropDuplicates(["product_id"])
    .fillna({"category": "unknown", "name_length": 0,
             "description_length": 0, "photos_qty": 0})
    .withColumn("dbt_updated_at", F.current_timestamp())
)
write_parquet(dim_products, "dim_products")


# ── dim_sellers ───────────────────────────────────────────────────────────────

print("[INFO] Building dim_sellers...")
dim_sellers = (
    sellers_df
    .select(
        F.col("seller_id"),
        F.col("seller_zip_code_prefix").cast("string").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
    )
    .dropDuplicates(["seller_id"])
    .withColumn("dbt_updated_at", F.current_timestamp())
)
write_parquet(dim_sellers, "dim_sellers")


# ── fct_orders ────────────────────────────────────────────────────────────────

print("[INFO] Building fct_orders...")

payments_agg = (
    payments_df
    .groupBy("order_id")
    .agg(
        F.sum(F.col("payment_value").cast(DoubleType())).alias("total_payment_value"),
        F.max(F.col("payment_installments").cast(IntegerType())).alias("max_installments"),
        F.collect_set("payment_type").alias("payment_types"),
    )
)

reviews_agg = (
    reviews_df
    .groupBy("order_id")
    .agg(F.avg(F.col("review_score").cast(DoubleType())).alias("avg_review_score"))
)

fct_orders = (
    order_items_df
    .join(orders_df,    on="order_id", how="inner")
    .join(payments_agg, on="order_id", how="left")
    .join(reviews_agg,  on="order_id", how="left")
    .withColumn("order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",
        F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",
        F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date"))
    .withColumn("shipping_limit_date",
        F.to_timestamp("shipping_limit_date"))
    .withColumn("price",         F.col("price").cast(DoubleType()))
    .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
    .withColumn("order_item_revenue",
        F.col("price") + F.col("freight_value"))
    .withColumn("delivery_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_purchase_timestamp")
        ).cast(IntegerType()))
    .withColumn("is_late_delivery",
        F.when(
            F.col("order_delivered_customer_date") >
            F.col("order_estimated_delivery_date"), 1
        ).otherwise(0))
    .withColumn("order_year_month",
        F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
    .select(
        "order_id", "order_item_id", "customer_id", "seller_id", "product_id",
        "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "shipping_limit_date",
        "price", "freight_value", "order_item_revenue",
        "total_payment_value", "max_installments", "payment_types",
        "avg_review_score", "delivery_days", "is_late_delivery",
        "order_year_month",
    )
    .fillna({"avg_review_score": 0.0, "delivery_days": -1, "is_late_delivery": 0})
)
write_parquet(fct_orders, "fct_orders", partition_cols=["order_year_month"])

print("[INFO] All tables written successfully. Committing job.")
job.commit()
