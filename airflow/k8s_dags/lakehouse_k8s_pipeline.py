"""Bronze then Silver, each as its own Kubernetes pod, launched by Airflow on Kubernetes."""
import os
from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG

# The pod template sits next to this file in the Git repository.
POD_TEMPLATE = os.path.join(os.path.dirname(__file__), "pipeline_pod.yaml")


def pipeline_pod(task_id: str, module: str) -> KubernetesPodOperator:
    """One pipeline step = one pod running our image with a different command."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),       # pod names may not contain underscores
        namespace="airflow",
        pod_template_file=POD_TEMPLATE,
        cmds=["python3", "-m", module],
        get_logs=True,                          # copy the pod's output into the Airflow task log
        on_finish_action="delete_pod",          # tidy up once the step is done
        startup_timeout_seconds=300,
    )


with DAG(
    dag_id="lakehouse_k8s_pipeline",
    description="Bronze and Silver as Kubernetes pods.",
    schedule=None,                              # run only when triggered
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["lakehouse", "kubernetes"],
) as dag:
    bronze = pipeline_pod("bronze_ingest", "spark_jobs.bronze.ingest_sources")
    silver = pipeline_pod("silver_build", "spark_jobs.silver.build_silver")
    bronze >> silver