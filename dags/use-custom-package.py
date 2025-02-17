from airflow_operators.sampleOperator import SampleOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "use_custom_package_as_requirement",
    description="Use your custom package in DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["requirement"],
) as dag:

    list_requirements = BashOperator(
        task_id="list_requirements",
        bash_command = "pip list"
    )

    sample_task = SampleOperator(task_id="sample-task", name="foo_bar")

    list_requirements >> sample_task