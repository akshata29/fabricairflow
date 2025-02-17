from airflow_operators.sampleOperator import SampleOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    "custom_package.py",
     default_args=default_args,
    description='A simple custom package DAG',
    schedule_interval=None,
    catchup=False
) as dag:

    # Define the tasks
    sample_task = SampleOperator(
        task_id='sample-task',
        name='foo_bar'
    )

    # Set the task dependencies
    sample_task
