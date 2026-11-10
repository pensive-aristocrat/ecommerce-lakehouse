"""Silver layer: clean, type, deduplicate and flatten the latest Bronze batch.

Run from the project root:  python -m spark_jobs.silver.build_silver
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_jobs.common.spark_session import get_spark

AUDIT_COLUMNS = ["_source_system", "_batch_id", "_ingested_at"]


# ---------- reusable helpers ----------

def latest_batch(spark: SparkSession, table: str) -> DataFrame:
    """Read only the newest batch of a Bronze table, without audit columns."""
    df = spark.table(f"bronze.{table}")
    newest = df.agg(F.max("_batch_id")).first()[0]
    return df.filter(F.col("_batch_id") == newest).drop(*AUDIT_COLUMNS)


def lower_column_names(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])


def trim_strings(df: DataFrame) -> DataFrame:
    for name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(name, F.trim(F.col(name)))
    return df


def keep_latest(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """Deduplicate: keep one row per key, the one with the latest order_col."""
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (df.withColumn("_rn", F.row_number().over(window))
              .filter("_rn = 1").drop("_rn"))


def save(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Replace the whole Silver table, so re-running gives the same result."""
    df.writeTo(f"silver.{table}").createOrReplace()
    print(f"  silver.{table:<28} {spark.table(f'silver.{table}').count():>8,} rows")


# ---------- one function per Silver table ----------

def build_orders(spark: SparkSession) -> DataFrame:
    timestamp_cols = ["order_purchase_timestamp", "order_approved_at",
                      "order_delivered_carrier_date", "order_delivered_customer_date",
                      "order_estimated_delivery_date"]
    df = trim_strings(latest_batch(spark, "pg_orders"))
    for col in timestamp_cols:
        df = df.withColumn(col, F.to_timestamp(col))
    return (df.withColumn("order_status", F.lower("order_status"))
              .withColumn("delivered_late",
                          F.col("order_delivered_customer_date")
                          > F.col("order_estimated_delivery_date"))
              .dropDuplicates(["order_id"]))


def build_order_items(spark: SparkSession) -> DataFrame:
    df = trim_strings(latest_batch(spark, "pg_order_items"))
    return (df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
              .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
              .withColumn("price", F.col("price").cast("decimal(10,2)"))
              .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
              .dropDuplicates(["order_id", "order_item_id"]))


def build_products(spark: SparkSession) -> DataFrame:
    products = trim_strings(latest_batch(spark, "pg_products"))
    names = trim_strings(latest_batch(spark, "pg_product_category_translation"))
    # The source has spelling mistakes in two column names; fix them here.
    products = (products
                .withColumnRenamed("product_name_lenght", "product_name_length")
                .withColumnRenamed("product_description_lenght", "product_description_length"))
    return (products.join(names, "product_category_name", "left")
            .withColumn("product_category_name_english",
                        F.coalesce("product_category_name_english", F.lit("unknown")))
            .dropDuplicates(["product_id"]))


def build_sellers(spark: SparkSession) -> DataFrame:
    df = trim_strings(lower_column_names(latest_batch(spark, "ora_sellers")))
    return (df.withColumn("seller_city", F.lower("seller_city"))
              .withColumn("seller_state", F.upper("seller_state"))
              .dropDuplicates(["seller_id"]))


def build_payments(spark: SparkSession) -> DataFrame:
    df = trim_strings(lower_column_names(latest_batch(spark, "ora_order_payments")))
    return (df.withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
              .withColumn("payment_installments", F.col("payment_installments").cast("int"))
              .withColumn("payment_value", F.col("payment_value").cast("decimal(10,2)"))
              .withColumn("payment_type", F.lower("payment_type"))
              .dropDuplicates(["order_id", "payment_sequential"]))


def build_reviews(spark: SparkSession) -> DataFrame:
    """Flatten the nested MongoDB documents into plain columns."""
    df = latest_batch(spark, "mongo_reviews")
    flat = df.select(
        F.trim("review_id").alias("review_id"),
        F.trim("order_id").alias("order_id"),
        F.col("score").cast("int").alias("review_score"),
        F.col("comment.title").alias("comment_title"),
        F.col("comment.message").alias("comment_message"),
        F.to_timestamp("dates.created").alias("review_created_at"),
        F.to_timestamp("dates.answered").alias("review_answered_at"),
    ).withColumn("has_comment", F.col("comment_message").isNotNull())
    # One review can cover several orders, so the key is (review_id, order_id).
    return keep_latest(flat, ["review_id", "order_id"], "review_answered_at")


def build_customers_scd2(spark: SparkSession) -> None:
    """Keep customer history: close changed rows, then insert new versions."""
    incoming = trim_strings(latest_batch(spark, "pg_customers"))
    incoming = (incoming
                .withColumn("customer_city", F.lower("customer_city"))
                .withColumn("row_hash", F.sha2(F.concat_ws(
                    "||", "customer_unique_id", "customer_zip_code_prefix",
                    "customer_city", "customer_state"), 256))
                .dropDuplicates(["customer_id"]))
    incoming.createOrReplaceTempView("incoming_customers")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.customers_scd2 (
            customer_id STRING, customer_unique_id STRING,
            customer_zip_code_prefix STRING, customer_city STRING,
            customer_state STRING, row_hash STRING,
            valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN
        ) USING iceberg
    """)
    # Step 1: a current row whose values changed is closed.
    spark.sql("""
        MERGE INTO silver.customers_scd2 t
        USING incoming_customers s
        ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN
          UPDATE SET is_current = false, valid_to = current_timestamp()
    """)
    # Step 2: customers with no current row (new, or just closed) get one.
    spark.sql("""
        INSERT INTO silver.customers_scd2
        SELECT s.customer_id, s.customer_unique_id, s.customer_zip_code_prefix,
               s.customer_city, s.customer_state, s.row_hash,
               current_timestamp() AS valid_from,
               CAST(NULL AS TIMESTAMP) AS valid_to,
               true AS is_current
        FROM incoming_customers s
        LEFT JOIN silver.customers_scd2 t
          ON t.customer_id = s.customer_id AND t.is_current = true
        WHERE t.customer_id IS NULL
    """)
    total = spark.table("silver.customers_scd2").count()
    print(f"  silver.{'customers_scd2':<28} {total:>8,} rows (all versions)")


def main() -> None:
    spark = get_spark("silver-build")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    print("Building Silver tables")
    save(spark, build_orders(spark), "orders")
    save(spark, build_order_items(spark), "order_items")
    save(spark, build_products(spark), "products")
    save(spark, build_sellers(spark), "sellers")
    save(spark, build_payments(spark), "order_payments")
    save(spark, build_reviews(spark), "reviews")
    build_customers_scd2(spark)
    spark.stop()
    print("Silver layer done.")


if __name__ == "__main__":
    main()