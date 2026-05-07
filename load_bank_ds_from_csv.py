from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = Path("/home/derby/projects/bank_101_airflow")
VENV_PYTHON = PROJECT_DIR / "venv_air" / "bin" / "python"
ETL_SCRIPT = PROJECT_DIR / "scripts" / "load_ds.py"


with DAG(
    dag_id="load_bank_ds_from_csv",
    description="Загрузка банковских CSV-файлов в детальный слой DS",
    start_date=datetime(2026, 4, 25),
    schedule=None,
    catchup=False,
    tags=["bank", "etl", "ds"],
) as dag:

    load_ds_from_csv = BashOperator(
        task_id="load_ds_from_csv",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} {ETL_SCRIPT}",
    )