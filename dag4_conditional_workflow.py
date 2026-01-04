from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
)

def determine_branch(**context):
    execution_date = context["execution_date"]
    day = execution_date.weekday()  # Monday = 0, Sunday = 6

    if day <= 2:
        return "weekday_task"
    elif day <= 4:
        return "end_of_week_task"
    else:
        return "weekend_task"

def weekday_process():
    return {
        "task_type": "weekday",
        "message": "Processing weekday data"
    }

def end_of_week_process():
    return {
        "task_type": "end_of_week",
        "message": "Processing end-of-week summary"
    }

def weekend_process():
    return {
        "task_type": "weekend",
        "message": "Running weekend maintenance"
    }

start = EmptyOperator(
    task_id="start",
    dag=dag
)

branch = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=determine_branch,
    provide_context=True,
    dag=dag
)

weekday_task = PythonOperator(
    task_id="weekday_task",
    python_callable=weekday_process,
    dag=dag
)

weekday_followup = EmptyOperator(
    task_id="weekday_followup",
    dag=dag
)

end_of_week_task = PythonOperator(
    task_id="end_of_week_task",
    python_callable=end_of_week_process,
    dag=dag
)

end_of_week_followup = EmptyOperator(
    task_id="end_of_week_followup",
    dag=dag
)

weekend_task = PythonOperator(
    task_id="weekend_task",
    python_callable=weekend_process,
    dag=dag
)

weekend_followup = EmptyOperator(
    task_id="weekend_followup",
    dag=dag
)

end = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag
)

start >> branch

branch >> weekday_task >> weekday_followup >> end
branch >> end_of_week_task >> end_of_week_followup >> end
branch >> weekend_task >> weekend_followup >> end
