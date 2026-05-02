from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor


# AIRBYTE_CONNECTION_ID = Variable.get("airbyte_connection_id")
AIRBYTE_CONNECTION_ID = "1ab78180-0c2a-4d57-8d9d-ff1afee8375d"
ALERT_EMAIL = "alexanderchosenokon@gmail.com"
DBT_PROJECT_DIR = "/home/chosen/data_engineering/dbt/beejanRide"
GCP_KEY_PATH = "/home/chosen/data_engineering/dbt/beejanRide.json"
DBT_PROFILES_DIR = "/home/chosen/.dbt"


default_args = {
    "owner": "beejanride",
    "depends_on_past": False,
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay":timedelta(minutes=5),
    "execution_timeout":timedelta(minutes=30)
}


with DAG(
    dag_id="beejanride_elt_pipeline",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 4, 30),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["beejanride", "elt", "production"]
) as dag:

# This phase is for ingestion of data
# It calls the Airbyte Cloud API using the Client ID and Client Secret and returns a job_id.
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_cloud_connection",
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=True,
    )


    wait_for_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id="airbyte_cloud_connection",
        airbyte_job_id=trigger_sync.output,
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

# this phase is to check for source freshness
    source_freshness = BashOperator(
    task_id="dbt_source_freshness",
    bash_command=(
        f"source ~/.bashrc && "
        f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt source freshness "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    ),
)

# this phase is the staging layer

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


# This is for the intermediate layer

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

# this phase runs all the test on the intermediate layer to avoid moving corrupt data to the mart layer

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

# this phase builds the core star schema layer and runs test too

    build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=(
            f"source ~/.bashrc && "
            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt build --select mart.core"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

# this phase is responsible for the mart layer build and all 3 layers are built simultaneously

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

# this phase is for the dbt snapshot but due to billing issue, i have to commment it out

#    run_snapshot = BashOperator(
#        task_id="dbt_run_snapshot",
#        bash_command=(
#            f"source ~/.bashrc && "
#            f"export GOOGLE_APPLICATION_CREDENTIALS='{GCP_KEY_PATH}' && "
#            f"cd {DBT_PROJECT_DIR} && "
#            f"dbt snapshot"
#            f" --project-dir {DBT_PROJECT_DIR}"
#            f" --profiles-dir {DBT_PROFILES_DIR}"
#        ),
#    )

# this phase sends an email notification to the alert email after a successful completion

    notify_success = EmailOperator(
        task_id="email_success_notification",
        to=ALERT_EMAIL,
        subject="BeejanRide Pipeline SUCCESS — {{ ds }}",
        html_content="""
            <h3>BeejanRide ELT Pipeline Completed Successfully</h3>
            <p><b>DAG:</b> {{ dag.dag_id }}</p>
            <p><b>Run ID:</b> {{ run_id }}</p>
            <p><b>Execution Date:</b> {{ ds }}</p>
            <p>All models built and tested. BigQuery is up to date.</p>
        """,
        conn_id="smtp_connect"
    )

    (
        trigger_sync
        >> wait_for_sync
        >> source_freshness
        >> build_staging
        >> build_intermediate
        >> test_intermediate
        >> build_core
        >> [build_finance, build_operations, build_fraud]
        >> notify_success
    )