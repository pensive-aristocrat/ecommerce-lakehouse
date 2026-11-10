"""Bronze layer: copy every source table into Iceberg, unchanged, with audit columns.

Run from the project root:  python -m spark_jobs.bronze.ingest_sources
"""
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common.spark_session import get_spark

POSTGRES = {
    "url": "jdbc:postgresql://localhost:5433/shop",
    "user": "shop",
    "password": "shop",
    "driver": "org.postgresql.Driver",
}
ORACLE = {
    "url": "jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
    "user": "shop",
    "password": "shop",
    "driver": "oracle.jdbc.OracleDriver",
}
MONGO_URI = "mongodb://root:root@localhost:27017/?authSource=admin"

POSTGRES_TABLES = ["customers", "orders", "order_items", "products",
                   "product_category_translation"]
ORACLE_TABLES = ["SELLERS", "ORDER_PAYMENTS"]  # Oracle stores names in upper case


def read_jdbc(spark: SparkSession, conn: dict, table: str) -> DataFrame:
    return spark.read.format("jdbc").options(**conn).option("dbtable", table).load()


def read_mongo(spark: SparkSession, database: str, collection: str) -> DataFrame:
    return (spark.read.format("mongodb")
            .option("connection.uri", MONGO_URI)
            .option("database", database)
            .option("collection", collection)
            .load())


def add_audit_columns(df: DataFrame, source: str, batch_id: str) -> DataFrame:
    """Record where each row came from and when, without touching the data."""
    return (df.withColumn("_source_system", F.lit(source))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_ingested_at", F.current_timestamp()))


def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> int:
    """Append to the Bronze table, creating it on the first run."""
    full_name = f"bronze.{table}"
    rows = df.count()
    if spark.catalog.tableExists(full_name):
        df.writeTo(full_name).append()
    else:
        df.writeTo(full_name).create()
    print(f"  {full_name:<40} {rows:>8,} rows")
    return rows


def main() -> None:
    spark = get_spark("bronze-ingest")
    # Batch IDs start with a UTC timestamp, so sorting them sorts by time.
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    print(f"Bronze batch {batch_id}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    for table in POSTGRES_TABLES:
        df = add_audit_columns(read_jdbc(spark, POSTGRES, table), "postgres", batch_id)
        write_bronze(spark, df, f"pg_{table}")

    for table in ORACLE_TABLES:
        df = add_audit_columns(read_jdbc(spark, ORACLE, table), "oracle", batch_id)
        write_bronze(spark, df, f"ora_{table.lower()}")

    reviews = add_audit_columns(read_mongo(spark, "shop", "reviews"), "mongodb", batch_id)
    write_bronze(spark, reviews, "mongo_reviews")

    spark.stop()
    print("Bronze layer done.")


if __name__ == "__main__":
    main()