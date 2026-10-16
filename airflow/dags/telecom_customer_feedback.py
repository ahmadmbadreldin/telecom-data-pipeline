import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# =========================================================
# Configuration
# =========================================================

SPARK_IMAGE = "apache/spark:3.5.7"
AIRFLOW_IMAGE = "apache/airflow:2.10.5"
DBT_IMAGE = "telecom-dbt:2.0.8"

SPARK_MASTER = "spark://spark-master:7077"

DOCKER_URL = "unix://var/run/docker.sock"
NETWORK_MODE = "telecom-data-pipeline_telecom_network"

SOURCE_DIR = "E:/telc/telecom-data-pipeline/src"
DATA_DIR = "E:/telc/telecom-data-pipeline/data"
SPARK_JARS_DIR = "E:/telc/telecom-data-pipeline/spark-jars"

DBT_PROJECT_DIR = (
    "E:/telc/telecom-data-pipeline/"
    "dbt/telecom_analytics"
)

DBT_PROFILE_FILE = (
    "C:/Users/w10mo/.dbt/profiles.yml"
)


# =========================================================
# Shared Mounts
# =========================================================

SOURCE_MOUNT = Mount(
    source=SOURCE_DIR,
    target="/opt/telecom/src",
    type="bind",
)

DATA_MOUNT = Mount(
    source=DATA_DIR,
    target="/opt/telecom/data",
    type="bind",
)

SPARK_JARS_MOUNT = Mount(
    source=SPARK_JARS_DIR,
    target="/opt/telecom/spark-jars",
    type="bind",
    read_only=True,
)

AZURE_CERT_MOUNT = Mount(
    source=(
        "C:/Users/w10mo/AppData/Roaming/"
        "TelecomPipeline/secrets/azure-uploader.pfx"
    ),
    target="/run/secrets/azure-uploader.pfx",
    type="bind",
    read_only=True,
)

DBT_PROJECT_MOUNT = Mount(
    source=DBT_PROJECT_DIR,
    target="/usr/app",
    type="bind",
)

DBT_PROFILE_MOUNT = Mount(
    source=DBT_PROFILE_FILE,
    target="/root/.dbt/profiles.yml",
    type="bind",
    read_only=True,
)


# =========================================================
# Snowflake Loader
# =========================================================

def load_customer_feedback_to_snowflake(
    ts_nodash,
    **context,
):

    # Lazy import to avoid Snowflake/Pandas loading during DAG parsing
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    datasets = {
        "STG_COMPLAINTS": (
            f"batch/runs/"
            f"customer_feedback_{ts_nodash}/"
            f"silver/complaints/"
        ),

        "STG_CUSTOMER_EXPERIENCE": (
            f"batch/runs/"
            f"customer_feedback_{ts_nodash}/"
            f"silver/customer_experience/"
        ),
    }

    hook.run([
        "USE WAREHOUSE TELECOM_WH",
        "USE DATABASE TELECOM_DWH",
        "USE SCHEMA STAGING",
    ])

    for table_name, stage_path in datasets.items():

        stage_location = (
            "@TELECOM_DWH.STAGING."
            "AZURE_SILVER_STAGE/"
            f"{stage_path}"
        )

        target_table = (
            f"TELECOM_DWH.STAGING.{table_name}"
        )

        print("=" * 70)
        print(
            f"CUSTOMER FEEDBACK -> {table_name}"
        )
        print("=" * 70)
        print(
            f"Source: {stage_location}"
        )
        print(
            f"Target: {target_table}"
        )
        print("=" * 70)

        # =================================================
        # Create table from Parquet schema
        # =================================================

        hook.run(
            f"""
            CREATE OR REPLACE TABLE {target_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(
                    OBJECT_CONSTRUCT(*)
                )
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '{stage_location}',
                        FILE_FORMAT =>
                            'TELECOM_DWH.STAGING.PARQUET_FORMAT'
                    )
                )
            )
            """
        )

        # =================================================
        # Load current run
        # =================================================

        hook.run(
            f"""
            COPY INTO {target_table}
            FROM {stage_location}
            FILE_FORMAT = (
                FORMAT_NAME =
                    'TELECOM_DWH.STAGING.PARQUET_FORMAT'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT'
            """
        )

        row_count = hook.get_first(
            f"SELECT COUNT(*) FROM {target_table}"
        )[0]

        print(
            f"[SUCCESS] {table_name} loaded "
            f"with {row_count:,} rows"
        )

    print("=" * 70)
    print(
        "CUSTOMER FEEDBACK SNOWFLAKE LOAD COMPLETED"
    )
    print("=" * 70)


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="telecom_customer_feedback",

    description=(
        "End-to-End Customer Feedback Pipeline "
        "for Complaints and Customer Experience"
    ),

    start_date=datetime(
        2026,
        10,
        13,
    ),

    # Run every hour
    schedule="30 * * * *",

    catchup=False,

    # Prevent two hourly runs from executing together
    max_active_runs=1,

    tags=[
        "telecom",
        "customer-feedback",
        "complaints",
        "customer-experience",
        "spark",
        "azure",
        "snowflake",
        "dbt",
    ],

) as dag:

    # =====================================================
    # 1. Clean Customer Feedback
    # =====================================================

    clean_customer_feedback = DockerOperator(
        task_id="clean_customer_feedback",

        image=SPARK_IMAGE,
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--jars "
            "/opt/telecom/spark-jars/"
            "hadoop-aws-3.3.4.jar,"
            "/opt/telecom/spark-jars/"
            "aws-java-sdk-bundle-1.12.262.jar "
            "/opt/telecom/src/"
            "telecom_pipeline/processing/silver/"
            "clean_customer_feedback.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "customer_feedback_{{ ts_nodash }}",

            "TELECOM_DATA_ROOT":
                "/opt/telecom/data",
        },

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            SOURCE_MOUNT,
            DATA_MOUNT,
            SPARK_JARS_MOUNT,
        ],
    )


    # =====================================================
    # 2. Verify Customer Feedback
    # =====================================================

    verify_customer_feedback = DockerOperator(
        task_id="verify_customer_feedback",

        image=SPARK_IMAGE,
        user="root",

        command=(
            "/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--jars "
            "/opt/telecom/spark-jars/"
            "hadoop-aws-3.3.4.jar,"
            "/opt/telecom/spark-jars/"
            "aws-java-sdk-bundle-1.12.262.jar "
            "/opt/telecom/src/"
            "telecom_pipeline/processing/silver/"
            "verify_customer_feedback.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "customer_feedback_{{ ts_nodash }}",

            "TELECOM_DATA_ROOT":
                "/opt/telecom/data",
        },

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            SOURCE_MOUNT,
            DATA_MOUNT,
            SPARK_JARS_MOUNT,
        ],
    )


    # =====================================================
    # 3. Mark Verified
    # =====================================================

    mark_verified = DockerOperator(
        task_id="mark_verified",

        image=AIRFLOW_IMAGE,
        user="root",

        command=(
            "python "
            "/opt/telecom/src/"
            "telecom_pipeline/ingestion/"
            "upload_to_azure.py "
            "mark-verified "
            "--run-id "
            "customer_feedback_{{ ts_nodash }} "
            "--group customer_feedback "
            "--data-root "
            "/opt/telecom/data"
        ),

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            SOURCE_MOUNT,
            DATA_MOUNT,
        ],
    )


    # =====================================================
    # 4. Upload to Azure
    # =====================================================

    upload_azure = DockerOperator(
        task_id="upload_azure",

        image=AIRFLOW_IMAGE,
        user="root",

        command=(
            "python "
            "/opt/telecom/src/"
            "telecom_pipeline/ingestion/"
            "upload_to_azure.py "
            "upload "
            "--run-id "
            "customer_feedback_{{ ts_nodash }} "
            "--group customer_feedback "
            "--data-root "
            "/opt/telecom/data "
            "--certificate "
            "/run/secrets/azure-uploader.pfx"
        ),

        private_environment={
            "TELECOM_AZURE_CERT_PASSWORD":
                os.environ.get(
                    "TELECOM_AZURE_CERT_PASSWORD",
                    "",
                ),
        },

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            SOURCE_MOUNT,
            DATA_MOUNT,
            AZURE_CERT_MOUNT,
        ],
    )


    # =====================================================
    # 5. Azure -> Snowflake
    #
    # STG_COMPLAINTS
    # STG_CUSTOMER_EXPERIENCE
    # =====================================================

    load_customer_feedback_snowflake = PythonOperator(
        task_id="load_customer_feedback_snowflake",

        python_callable=(
            load_customer_feedback_to_snowflake
        ),

        op_kwargs={
            "ts_nodash":
                "{{ ts_nodash }}",
        },
    )


    # =====================================================
    # 6. Selective dbt Transform
    #
    # Only models downstream from:
    #
    # STG_COMPLAINTS
    # STG_CUSTOMER_EXPERIENCE
    #
    # Result:
    # FACT_CUSTOMER_COMPLAINT
    # FACT_CUSTOMER_EXPERIENCE
    # =====================================================

    dbt_customer_feedback_transform = DockerOperator(
        task_id="dbt_customer_feedback_transform",

        image=DBT_IMAGE,

        pool="dbt_pool",

        command=[
            "dbt",
            "run",

            "--project-dir",
            "/usr/app",

            "--profiles-dir",
            "/root/.dbt",

            "--select",
            "source:telecom_staging.stg_complaints+",
            "source:telecom_staging.stg_customer_experience+",
        ],

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            DBT_PROJECT_MOUNT,
            DBT_PROFILE_MOUNT,
        ],
    )


    # =====================================================
    # 7. Selective dbt Tests
    # =====================================================

    dbt_customer_feedback_test = DockerOperator(
        task_id="dbt_customer_feedback_test",

        image=DBT_IMAGE,

        pool="dbt_pool",

        command=[
            "dbt",
            "test",

            "--project-dir",
            "/usr/app",

            "--profiles-dir",
            "/root/.dbt",

            "--select",
            "source:telecom_staging.stg_complaints+",
            "source:telecom_staging.stg_customer_experience+",
        ],

        docker_url=DOCKER_URL,

        network_mode=NETWORK_MODE,

        auto_remove="success",

        mount_tmp_dir=False,

        mounts=[
            DBT_PROJECT_MOUNT,
            DBT_PROFILE_MOUNT,
        ],
    )


    # =====================================================
    # Pipeline Dependency
    # =====================================================

    (
        clean_customer_feedback

        >> verify_customer_feedback

        >> mark_verified

        >> upload_azure

        >> load_customer_feedback_snowflake

        >> dbt_customer_feedback_transform

        >> dbt_customer_feedback_test
    )