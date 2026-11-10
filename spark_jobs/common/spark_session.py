import os
from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.1"

PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "org.postgresql:postgresql:42.7.4",                     # PostgreSQL JDBC driver
    "com.oracle.database.jdbc:ojdbc11:23.5.0.24.07",        # Oracle JDBC driver
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",  # MongoDB connector
])


def get_spark(app_name: str) -> SparkSession:
    """Create a SparkSession connected to the Polaris Iceberg catalog."""
    # Read secrets from environment variables; fall back to local defaults.
    credential = os.getenv("POLARIS_CREDENTIAL", "root:s3cr3t")
    warehouse = os.getenv("POLARIS_WAREHOUSE", "quickstart_catalog")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")  # use all CPU cores on this machine
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", "http://localhost:8181/api/catalog")
        .config("spark.sql.catalog.polaris.warehouse", warehouse)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.token-refresh-enabled", "false")
        .config("spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation",
                "vended-credentials")
        .config("spark.sql.catalog.polaris.client.region", "irrelevant")
        .config("spark.sql.defaultCatalog", "polaris")
        .config("spark.sql.shuffle.partitions", "8")  # small laptop, small number
        .getOrCreate()
    )