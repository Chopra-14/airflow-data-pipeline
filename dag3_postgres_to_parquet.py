from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

dag = DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False
)

def check_table_exists(table_name):
    hook = PostgresHook()
    records = hook.get_records(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name='{table_name}'"
    )
    if records[0][0] == 0:
        raise ValueError(f"Table {table_name} does not exist")

    count = hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
    if count[0][0] == 0:
        raise ValueError("Table exists but contains no data")

    return True

def export_table_to_parquet(table_name, output_path):
    hook = PostgresHook()
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy"
    )

    return {
        "file_path": output_path,
        "row_count": len(df),
        "file_size_bytes": os.path.getsize(output_path)
    }

def validate_parquet(file_path):
    df = pd.read_parquet(file_path)
    if df.empty:
        raise ValueError("Parquet file is empty")
    return True

check_task = PythonOperator(
    task_id="check_source_table_exists",
    python_callable=check_table_exists,
    op_kwargs={"table_name": "transformed_employee_data"},
    dag=dag
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_table_to_parquet,
    op_kwargs={
        "table_name": "transformed_employee_data",
        "output_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet"
    },
    dag=dag
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    op_kwargs={
        "file_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet"
    },
    dag=dag
)

check_task >> export_task >> validate_task
