from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


BATCH_PATH = Path("/opt/telecom/data/raw/batch")

REQUIRED_BATCH_FILES = [
    "areas.csv",
    "bridge_incident_customer.csv",
    "complaints.csv",
    "complaint_types.csv",
    "customers.csv",
    "customer_experience.csv",
    "incident_types.csv",
    "network_incidents.csv",
    "plans.csv",
    "services.csv",
    "towers.csv",
]


def check_batch_files():
    missing_files = []

    for file_name in REQUIRED_BATCH_FILES:
        file_path = BATCH_PATH / file_name

        if not file_path.exists():
            missing_files.append(file_name)

    if missing_files:
        raise FileNotFoundError(
            f"Missing batch source files: {missing_files}"
        )

    print("===================================")
    print("Batch Source Validation Successful")
    print("===================================")

    for file_name in REQUIRED_BATCH_FILES:
        file_path = BATCH_PATH / file_name
        size_mb = file_path.stat().st_size / (1024 * 1024)

        print(f"{file_name}: {size_mb:.2f} MB")

    print(f"Total files validated: {len(REQUIRED_BATCH_FILES)}")


with DAG(
    dag_id="telecom_batch_pipeline",
    description="End-to-End Telecom Batch Data Pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["telecom", "batch", "data-engineering"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_batch_sources = PythonOperator(
        task_id="check_batch_sources",
        python_callable=check_batch_files
    )

    spark_customers = DockerOperator(
        task_id="spark_customers",

        image="apache/spark:3.5.7",
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/telecom/src/telecom_pipeline/processing/silver/clean_customers.py"
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode="telecom-data-pipeline_telecom_network",

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source="E:/telc/telecom-data-pipeline/src",
                target="/opt/telecom/src",
                type="bind"
            ),
            Mount(
                source="E:/telc/telecom-data-pipeline/data",
                target="/opt/telecom/data",
                type="bind"
            ),
        ],
    )
    spark_network_reference = DockerOperator(
        task_id="spark_network_reference",

        image="apache/spark:3.5.7",
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/telecom/src/telecom_pipeline/processing/silver/clean_network_reference.py"
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode="telecom-data-pipeline_telecom_network",

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source="E:/telc/telecom-data-pipeline/src",
                target="/opt/telecom/src",
                type="bind"
            ),
            Mount(
                source="E:/telc/telecom-data-pipeline/data",
                target="/opt/telecom/data",
                type="bind"
            ),
        ],
    )
    spark_reference_tables = DockerOperator(
        task_id="spark_reference_tables",

        image="apache/spark:3.5.7",
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/telecom/src/telecom_pipeline/processing/silver/clean_reference_tables.py"
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode="telecom-data-pipeline_telecom_network",

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source="E:/telc/telecom-data-pipeline/src",
                target="/opt/telecom/src",
                type="bind"
            ),
            Mount(
                source="E:/telc/telecom-data-pipeline/data",
                target="/opt/telecom/data",
                type="bind"
            ),
        ],
    )
    spark_customer_feedback = DockerOperator(
        task_id="spark_customer_feedback",

        image="apache/spark:3.5.7",
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/telecom/src/telecom_pipeline/processing/silver/clean_customer_feedback.py"
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode="telecom-data-pipeline_telecom_network",

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source="E:/telc/telecom-data-pipeline/src",
                target="/opt/telecom/src",
                type="bind"
            ),
            Mount(
                source="E:/telc/telecom-data-pipeline/data",
                target="/opt/telecom/data",
                type="bind"
            ),
        ],
    )

    spark_incident_impact = DockerOperator(
        task_id="spark_incident_impact",

        image="apache/spark:3.5.7",
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4 "
            "/opt/telecom/src/telecom_pipeline/processing/silver/clean_incident_impact.py"
        ),

        docker_url="unix://var/run/docker.sock",

        network_mode="telecom-data-pipeline_telecom_network",

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            Mount(
                source="E:/telc/telecom-data-pipeline/src",
                target="/opt/telecom/src",
                type="bind"
            ),
            Mount(
                source="E:/telc/telecom-data-pipeline/data",
                target="/opt/telecom/data",
                type="bind"
            ),
        ],
    )

    validate_silver = EmptyOperator(
        task_id="validate_silver"
    )

    load_snowflake = EmptyOperator(
        task_id="load_snowflake"
    )

    dbt_transform = EmptyOperator(
        task_id="dbt_transform"
    )

    dbt_test = EmptyOperator(
        task_id="dbt_test"
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> check_batch_sources
        >> spark_customers
        >> spark_network_reference
        >> spark_reference_tables
        >> spark_customer_feedback
        >> spark_incident_impact
        >> validate_silver
        >> load_snowflake
        >> dbt_transform
        >> dbt_test
        >> end
    )