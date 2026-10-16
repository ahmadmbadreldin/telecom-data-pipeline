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

def load_incident_impact_to_snowflake(
    ts_nodash,
    **context,
):
    # Lazy import:
    # prevents loading Snowflake/Pandas during DAG parsing
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    datasets = {
        "STG_NETWORK_INCIDENTS_REFERENCE": (
            f"batch/runs/"
            f"incident_impact_{ts_nodash}/"
            f"silver/network_incidents_reference/"
        ),

        "STG_BRIDGE_INCIDENT_CUSTOMER": (
            f"batch/runs/"
            f"incident_impact_{ts_nodash}/"
            f"silver/bridge_incident_customer/"
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
        print(f"INCIDENT IMPACT -> {table_name}")
        print("=" * 70)
        print(f"Source: {stage_location}")
        print(f"Target: {target_table}")
        print("=" * 70)

        # =================================================
        # Create / Replace table from Parquet schema
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
    print("INCIDENT IMPACT SNOWFLAKE LOAD COMPLETED")
    print("=" * 70)


# =========================================================
# DAG Definition
# =========================================================

with DAG(
    dag_id="telecom_incident_impact",

    description=(
        "End-to-End Network Incident and "
        "Customer Impact Pipeline"
    ),

    start_date=datetime(
        2026,
        10,
        13,
    ),

    # Run every hour at minute 00
    schedule="0 * * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "telecom",
        "incidents",
        "customer-impact",
        "spark",
        "azure",
        "snowflake",
        "dbt",
    ],

) as dag:

    # =====================================================
    # 1. Clean Incident Impact
    # =====================================================

    clean_incident_impact = DockerOperator(
        task_id="clean_incident_impact",

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
            "clean_incident_impact.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "incident_impact_{{ ts_nodash }}",

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
    # 2. Verify Incident Impact
    # =====================================================

    verify_incident_impact = DockerOperator(
        task_id="verify_incident_impact",

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
            "verify_incident_impact.py"
        ),

        environment={
            "TELECOM_RUN_ID":
                "incident_impact_{{ ts_nodash }}",

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
            "incident_impact_{{ ts_nodash }} "
            "--group incident_impact "
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
            "incident_impact_{{ ts_nodash }} "
            "--group incident_impact "
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
    # STG_NETWORK_INCIDENTS_REFERENCE
    # STG_BRIDGE_INCIDENT_CUSTOMER
    # =====================================================

    load_incident_impact_snowflake = PythonOperator(
        task_id="load_incident_impact_snowflake",

        python_callable=(
            load_incident_impact_to_snowflake
        ),

        op_kwargs={
            "ts_nodash":
                "{{ ts_nodash }}",
        },
    )


    # =====================================================
    # 6. Selective dbt Transform
    # =====================================================

    dbt_incident_impact_transform = DockerOperator(
        task_id="dbt_incident_impact_transform",

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
            (
                "source:telecom_staging."
                "stg_network_incidents_reference+ "
                "source:telecom_staging."
                "stg_bridge_incident_customer+"
            ),
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

    dbt_incident_impact_test = DockerOperator(
        task_id="dbt_incident_impact_test",

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
            (
                "source:telecom_staging."
                "stg_network_incidents_reference+ "
                "source:telecom_staging."
                "stg_bridge_incident_customer+"
            ),
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
        clean_incident_impact
        >> verify_incident_impact
        >> mark_verified
        >> upload_azure
        >> load_incident_impact_snowflake
        >> dbt_incident_impact_transform
        >> dbt_incident_impact_test
    )