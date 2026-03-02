from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator


def alert_on_failure(context):
    """
    Alert simple: deja un log bien visible cuando una task falla.
    Después podés reemplazar esto por Slack/Email sin tocar el DAG.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url

    logging.error(
        "🚨 ALERTA PIPELINE FALLÓ | dag=%s | task=%s | run_id=%s | logs=%s",
        dag_id, task_id, run_id, log_url
    )

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="olist_oltp_to_olap",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="0 3 * * *",  # todos los días 03:00
    catchup=False,
    tags=["portfolio", "data-engineering", "oltp-olap"],
) as dag:

    load_oltp = BashOperator(
        task_id="load_oltp",
        bash_command="python /opt/airflow/src/load_oltp.py",
    )

    create_olap = BashOperator(
        task_id="create_olap_tables",
        bash_command="psql postgresql://olist_user:olist_pass@postgres:5432/olist -f /opt/airflow/sql/02_olap/01_create_analytics_tables.sql",
    )

    populate_dims = BashOperator(
        task_id="populate_dims",
        bash_command="psql postgresql://olist_user:olist_pass@postgres:5432/olist -f /opt/airflow/sql/02_olap/02_populate_dims.sql",
    )

    populate_fact = BashOperator(
    task_id="populate_fact",
    bash_command="psql postgresql://olist_user:olist_pass@postgres:5432/olist -f /opt/airflow/sql/02_olap/03_populate_fact.sql",
)

data_quality_gate = BashOperator(
    task_id="data_quality_gate",
    bash_command=(
        "psql postgresql://olist_user:olist_pass@postgres:5432/olist "
        "-f /opt/airflow/sql/04_tests/quality_gate.sql"
    ),
)

run_tests = BashOperator(
    task_id="run_tests",
    bash_command="psql postgresql://olist_user:olist_pass@postgres:5432/olist -f /opt/airflow/sql/04_tests/tests.sql",
)


load_oltp >> create_olap >> populate_dims >> populate_fact >> data_quality_gate >> run_tests