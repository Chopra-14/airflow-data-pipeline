from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)

def send_success_notification(context):
    return {
        "type": "success",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id
    }

def send_failure_notification(context):
    return {
        "type": "failure",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "error": str(context.get("exception"))
    }

def risky_operation(**context):
    day = context["execution_date"].day
    if day % 5 == 0:
        raise Exception("Simulated failure condition met")
    return {"status": "success"}

def cleanup_task():
    return {"cleanup": "completed"}

start = EmptyOperator(task_id="start", dag=dag)

risky = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag
)

success = EmptyOperator(
    task_id="success_notification",
    trigger_rule="all_success",
    dag=dag
)

failure = EmptyOperator(
    task_id="failure_notification",
    trigger_rule="all_failed",
    dag=dag
)

cleanup = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup_task,
    trigger_rule="all_done",
    dag=dag
)

start >> risky >> [success, failure]
[success, failure] >> cleanup
