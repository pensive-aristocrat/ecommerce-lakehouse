# The lakehouse Spark jobs, packaged to run anywhere: Docker, Kubernetes, the cloud.
FROM eclipse-temurin:17-jre-jammy

# Python to run PySpark (Java 17 comes with the base image).
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip3 install --no-cache-dir "pyspark>=3.5,<3.6"

COPY spark_jobs/ spark_jobs/

# Download Spark's Java libraries once, at build time, so every run starts fast.
RUN python3 -c "from pyspark.sql import SparkSession; \
from spark_jobs.common.spark_session import PACKAGES; \
SparkSession.builder.master('local[1]').config('spark.jars.packages', PACKAGES).getOrCreate().stop()"

ENV PYTHONUNBUFFERED=1
CMD ["python3", "-m", "spark_jobs.bronze.ingest_sources"]