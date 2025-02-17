from datetime import datetime
from airflow import DAG

 # Import from private package
from airflow_operator.sample_operator import SampleOperator


with DAG(
"test-custom-package",
tags=["example"]
description="A simple tutorial DAG",
schedule_interval=None,
start_date=datetime(2021, 1, 1),
) as dag:
    task = SampleOperator(task_id="sample-task", name="foo_bar")

    task