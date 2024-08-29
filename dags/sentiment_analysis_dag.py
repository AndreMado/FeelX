from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_control',
    default_args=default_args,
    description='Controla el flujo de datos ejecutando producer y consumer',
    schedule_interval='@hourly',
)

run_producer = BashOperator(
    task_id='run_producer',
    bash_command='docker exec -d diciembre27-producer-1 python /app/scripts/producer.py',
    dag=dag,
)

run_consumer = BashOperator(
    task_id='run_consumer',
    bash_command='docker exec -d diciembre27-spark-1 spark-submit /opt/bitnami/spark/scripts/consumer.py',
    dag=dag,
)

run_producer >> run_consumer
