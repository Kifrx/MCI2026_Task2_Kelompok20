from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mci_kelompok_20',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'ecommerce_orders_pipeline',
    default_args=default_args,
    schedule_interval='@hourly', # Narik data setiap 1 jam
    catchup=False,
    max_active_runs=1,
    description='Pipeline ETL Data Orders -> Spark -> ClickHouse'
) as dag:

    # ambil data
    ingest_api = BashOperator(
        task_id='fetch_orders_data',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders.py'
    )

    # Load  ClickHouse
    process_spark = BashOperator(
        task_id='process_to_clickhouse',
        bash_command='python -u /opt/airflow/dags/scripts/process_orders_spark.py'
    )

    ingest_api >> process_spark