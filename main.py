import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from pipeline_1 import main as ingest_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2024, 8, 26),
}


dag = DAG(
    'assignment',
    default_args=default_args,
    description="Assignment",
    schedule="19 00 * * *"
)


etl_1 = PythonOperator(
    task_id="data_ingestion",
    python_callable=ingest_data,
    dag=dag
)

etl_1.run()
