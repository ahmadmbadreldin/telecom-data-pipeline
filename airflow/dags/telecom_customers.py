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

def load_customers_to_snowflake(ts_nodash, **context):

    # Lazy import to avoid loading Snowflake/Pandas during DAG parsing
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    stage_location = (
        "@TELECOM_DWH.STAGING."
        "AZURE_SILVER_STAGE/"
        f"batch/runs/customers_{ts_nodash}/"
        "silver/customers/"
    )

    target_table = (
        "TELECOM_DWH.STAGING.STG_CUSTOMERS"
    )

    print("=" * 70)
    print("CUSTOMERS -> SNOWFLAKE")
    print("=" * 70)
    print(f"Source: {stage_location}")
    print(f"Target: {target_table}")
    print("=" * 70)

    hook.run([
        "USE WAREHOUSE TELECOM_WH",
        "USE DATABASE TELECOM_DWH",
        "USE SCHEMA STAGING",
    ])

    # -----------------------------------------------------
    # Recreate STG_CUSTOMERS using current Parquet schema
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # Load current Customers run
    # -----------------------------------------------------

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

    print("=" * 70)
    print(
        f"[SUCCESS] STG_CUSTOMERS loaded "
        f"with {row_count:,} rows"
    )
    print("=" * 70)


# =========================================================
# DAG Definition
# =========================================================

with DAG(
    dag_id="telecom_customers",

    description=(
        "End-to-End Telecom Customers Pipeline"
    ),

    start_date=datetime(2026, 10, 13),

    # Daily at 02:00 AM
    schedule="0 2 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "telecom",
        "customers",
        "spark",
        "azure",
        "snowflake",
        "dbt",
    ],

) as dag:

    # =====================================================
    # 1. Clean Customers
    # =====================================================

    clean_customers = DockerOperator(
        task_id="clean_customers",

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
            "clean_customers.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "customers_{{ ts_nodash }}",

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
    # 2. Verify Customers
    # =====================================================

    verify_customers = DockerOperator(
        task_id="verify_customers",

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
            "verify_customers.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "customers_{{ ts_nodash }}",

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
    # 3. Mark Customers Run Verified
    # =====================================================

    mark_customers_verified = DockerOperator(
        task_id="mark_customers_verified",

        image=AIRFLOW_IMAGE,
        user="root",

        command=(
            "python "
            "/opt/telecom/src/"
            "telecom_pipeline/ingestion/"
            "upload_to_azure.py "
            "mark-verified "
            "--run-id "
            "customers_{{ ts_nodash }} "
            "--group customers "
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
    # 4. Upload Customers to Azure
    # =====================================================

    upload_customers_azure = DockerOperator(
        task_id="upload_customers_azure",

        image=AIRFLOW_IMAGE,
        user="root",

        command=(
            "python "
            "/opt/telecom/src/"
            "telecom_pipeline/ingestion/"
            "upload_to_azure.py "
            "upload "
            "--run-id "
            "customers_{{ ts_nodash }} "
            "--group customers "
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
    # 5. Azure -> Snowflake STG_CUSTOMERS
    # =====================================================

    load_customers_snowflake = PythonOperator(
        task_id="load_customers_snowflake",

        python_callable=(
            load_customers_to_snowflake
        ),

        op_kwargs={
            "ts_nodash":
                "{{ ts_nodash }}",
        },
    )


    # =====================================================
    # 6. dbt Transform
    #
    # STG_CUSTOMERS
    #       ↓
    # DIM_CUSTOMER
    #       ↓
    # affected customer facts
    # =====================================================

    dbt_customers_transform = DockerOperator(
        task_id="dbt_customers_transform",

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
            "source:telecom_staging.stg_customers+",
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
    # 7. dbt Tests
    # =====================================================

    dbt_customers_test = DockerOperator(
        task_id="dbt_customers_test",

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
            "source:telecom_staging.stg_customers+",
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
    # Dependencies
    # =====================================================

    (
        clean_customers

        >> verify_customers

        >> mark_customers_verified

        >> upload_customers_azure

        >> load_customers_snowflake

        >> dbt_customers_transform

        >> dbt_customers_test
    )