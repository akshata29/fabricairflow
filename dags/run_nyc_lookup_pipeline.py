from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator


with DAG(
    dag_id="nyc_lookup_pipeline",
    start_date=datetime(2022, 5, 14),
    schedule_interval="@once",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure_data_factory_conn_id", #This is a connection created on Airflow UI
    },
    default_view="graph",
) as dag:

    run_adf_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id="run_adf_pipeline",
        pipeline_name="pl_stage_lookup",
    )

    run_adf_pipeline
