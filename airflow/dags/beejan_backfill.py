from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


ALERT_EMAIL = "alexanderchosenokon@gmail.com"
DBT_PROJECT_DIR = "/home/chosen/data_engineering/dbt/beejanRide"
DBT_PROFILES_DIR = "/home/chosen/.dbt"
GCP_KEY_PATH = "/home/chosen/data_engineering/dbt/beejanRide.json"


default_args = {
    "owner": "beejanride",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


with DAG(
    dag_id="beejanride_backfill",
    schedule="@daily",
    start_date=datetime(2026, 4, 30),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["beejanride", "backfill", "manual"],
) as dag:


    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt source freshness"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_staging = BashOperator(
        task_id="dbt_build_staging",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select staging"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_intermediate = BashOperator(
        task_id="dbt_build_intermediate",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select intermediate"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    test_intermediate = BashOperator(
        task_id="dbt_test_intermediate",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select intermediate"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select marts.core"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_finance = BashOperator(
        task_id="dbt_build_finance",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select tag:finance"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_operations = BashOperator(
        task_id="dbt_build_operations",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select tag:operations"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    build_fraud = BashOperator(
        task_id="dbt_build_fraud",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select tag:fraud"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    (
        source_freshness
        >> build_staging
        >> build_intermediate
        >> test_intermediate
        >> build_core
        >> [build_finance, build_operations, build_fraud]
    )