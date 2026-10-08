from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil

PROJECT_ROOT = "/usr/local/airflow"
DATA_DIR = os.path.join(PROJECT_ROOT, "include", "data")
EXTRACT_DIR = os.path.join(PROJECT_ROOT, "include", "extracted")
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, "include", "sales_dbt")
DBT_PROFILES_DIR = DBT_PROJECT_DIR


default_args = {
    "owner": "ziad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

}

def extract_data():
    os.makedirs(EXTRACT_DIR, exist_ok=True)
    files = ["accounts.csv", "products.csv", "sales_teams.csv", "sales_pipeline.csv"]
    for file_name in files:
        source_path = os.path.join(DATA_DIR, file_name)
        destination_path = os.path.join(EXTRACT_DIR, file_name)
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")
        shutil.copy2(source_path, destination_path)
        print(f"Extracted: {file_name}")


def load_data():
    import duckdb
    DB_PATH = os.path.join(PROJECT_ROOT, "include", "sales_dw.duckdb")
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    for file_name, table_name in [
        ("accounts.csv", "accounts"),
        ("products.csv", "products"),
        ("sales_teams.csv", "sales_teams"),
        ("sales_pipeline.csv", "sales_pipeline")
    ]:
        path = os.path.join(EXTRACT_DIR, file_name)
        df = pd.read_csv(path)
        if file_name == "sales_pipeline.csv":
            df["engage_date"] = pd.to_datetime(df["engage_date"], errors="coerce")
            df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")
            
        conn.execute(f"CREATE OR REPLACE TABLE raw.{table_name} AS SELECT * FROM df")
        
    conn.close()



with DAG(
    dag_id="astro_duckdb_dbt_etl",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "duckdb", "dbt", "sales"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps"
    )


    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )


    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    extract_task >> load_task >> dbt_deps_task >> dbt_run_task >> dbt_test_task