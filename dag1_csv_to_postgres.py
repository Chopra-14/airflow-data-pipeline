from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)

def create_employee_table():
    hook = PostgresHook()
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            join_date DATE
        );
    """)

def truncate_employee_table():
    PostgresHook().run("TRUNCATE TABLE raw_employee_data;")

def load_csv_data():
    df = pd.read_csv("/opt/airflow/data/input.csv")
    engine = PostgresHook().get_sqlalchemy_engine()
    df.to_sql(
        "raw_employee_data",
        engine,
        if_exists="append",
        index=False
    )
    return len(df)

create_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag
)

truncate_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_data,
    dag=dag
)

create_task >> truncate_task >> load_task
