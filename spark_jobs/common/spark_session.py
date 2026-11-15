import os

from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.10.1"
OPENLINEAGE_VERSION = "1.48.0"

PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    "org.postgresql:postgresql:42.7.4",                     # PostgreSQL JDBC driver
    "com.oracle.database.jdbc:ojdbc11:23.5.0.24.07",        # Oracle JDBC driver
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",  # MongoDB connector
    f"io.openlineage:openlineage-spark_2.12:{OPENLINEAGE_VERSION}",  # lineage listener
])


def get_spark(app_name: str) -> SparkSession:
    """Create a SparkSession connected to the Polaris Iceberg catalog.

    On the laptop every setting has a default. Inside a container (Docker or
    Kubernetes) the addresses come from environment variables instead.
    """
    credential = os.getenv("POLARIS_CREDENTIAL", "root:s3cr3t")
    warehouse = os.getenv("POLARIS_WAREHOUSE", "quickstart_catalog")
    polaris_uri = os.getenv("POLARIS_URI", "http://localhost:8181/api/catalog")
    lineage_url = os.getenv("OPENLINEAGE_URL", "http://localhost:5000")
    s3_endpoint = os.getenv("S3_ENDPOINT")  # set only inside containers

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type", "rest")
        .config("spark.sql.catalog.polaris.uri", polaris_uri)
        .config("spark.sql.catalog.polaris.warehouse", warehouse)
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.token-refresh-enabled", "false")
        .config("spark.sql.catalog.polaris.client.region", "irrelevant")
        .config("spark.sql.defaultCatalog", "polaris")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", lineage_url)
        .config("spark.openlineage.namespace", "lakehouse")
    )

    if s3_endpoint:
        # Inside a container, "localhost" means the container itself, so the
        # storage address Polaris hands out would point nowhere. Use fixed
        # RustFS keys and our own endpoint instead (like Trino in Module 8).
        builder = (
            builder
            .config("spark.sql.catalog.polaris.s3.endpoint", s3_endpoint)
            .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
            .config("spark.sql.catalog.polaris.s3.access-key-id", os.environ["S3_ACCESS_KEY"])
            .config("spark.sql.catalog.polaris.s3.secret-access-key", os.environ["S3_SECRET_KEY"])
        )
    else:
        # On the laptop, Polaris hands Spark short-lived storage credentials.
        builder = builder.config(
            "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation", "vended-credentials")

    return builder.getOrCreate()