from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


PROJECT_DIR = Path("/home/derby/projects/bank_101_airflow")
SQL_FILE = PROJECT_DIR / "sql" / "create_ds_tables.sql"


with DAG(
    dag_id="init_bank_ds_tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bank", "init"],
) as dag:

    create_tables = SQLExecuteQueryOperator(
        task_id="create_ds_and_logs_tables",
        conn_id="bank_postgres",
        sql=SQL_FILE.read_text(encoding="utf-8"),
    )