# Superset 7 plus the Trino driver.
FROM apache/superset:6.0.0

USER root
# Superset keeps its Python packages in /app/.venv; install the driver there.
RUN . /app/.venv/bin/activate && uv pip install trino
USER superset