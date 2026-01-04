from airflow.models import DagBag

def test_dag2_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "data_transformation_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag2_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["data_transformation_pipeline"]
    assert len(dag.tasks) == 2

def test_dag2_dependencies():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["data_transformation_pipeline"]
    create = dag.get_task("create_transformed_table")
    transform = dag.get_task("transform_and_load")

    assert transform in create.downstream_list

def test_dag2_schedule():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["data_transformation_pipeline"]
    assert dag.schedule_interval == "@daily"

def test_dag2_no_cycles():
    dag = DagBag(dag_folder="dags", include_examples=False).dags["data_transformation_pipeline"]
    dag.test_cycle()
