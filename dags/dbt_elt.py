from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from pendulum import datetime

# Path to your dbt project inside the Airflow container
DBT_PROJECT_DIR = "/opt/airflow/dbt/dbt_stock_project"

# Load Snowflake connection  from Airflow connection
conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id="BuildELT_dbt",
    start_date=datetime(2025, 4, 1),
    schedule_interval="0 10 * * *",  # daily at 10 AM
    catchup=False,
    description="Run dbt models, tests, and snapshots to transform stock data in Snowflake",
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
    tags=["dbt", "elt", "stocks"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
    )

    # DAG task flow
    dbt_run >> dbt_test >> dbt_snapshot
