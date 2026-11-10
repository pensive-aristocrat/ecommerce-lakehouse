#!/usr/bin/env bash
# Submit the Scala Silver job to Spark (PySpark's Spark, in local mode).
set -euo pipefail

cd "$(dirname "$0")/.."
ICEBERG=1.10.1
spark-submit \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG,org.apache.iceberg:iceberg-aws-bundle:$ICEBERG" \
  --class SilverSellers \
  scala_jobs/target/scala-2.12/silver-scala_2.12-0.1.0.jar