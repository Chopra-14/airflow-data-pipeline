from airflow.models import DagBag

def test_dag1_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["csv_to_postgres_ingestion"]
    assert len(dag.tasks) == 3

def test_dag1_dependencies():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["csv_to_postgres_ingestion"]
    create = dag.get_task("create_table_if_not_exists")
    truncate = dag.get_task("truncate_table")
    load = dag.get_task("load_csv_to_postgres")

    assert truncate in create.downstream_list
    assert load in truncate.downstream_list

def test_dag1_schedule():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["csv_to_postgres_ingestion"]
    assert dag.schedule_interval == "@daily"

def test_dag1_no_cycles():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["csv_to_postgres_ingestion"]
    dag.test_cycle()
