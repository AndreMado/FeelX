from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'full_workflow_orchestration',
    default_args=default_args,
    description='Orchestrate Zookeeper, Kafka, Producer, and Spark',
    schedule_interval=None,  # Se ejecuta bajo demanda
)

# 1. Iniciar Zookeeper
start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='docker start diciembre27-zookeeper-1',  # O 'docker-compose up -d zookeeper'
    dag=dag,
)

# 2. Iniciar Kafka (después de Zookeeper)
start_kafka_1 = BashOperator(
    task_id='start_kafka_1',
    bash_command='docker start diciembre27-kafka-1-1',  # O 'docker-compose up -d kafka-1'
    dag=dag,
)

start_kafka_2 = BashOperator(
    task_id='start_kafka_2',
    bash_command='docker start diciembre27-kafka-2-1',  # O 'docker-compose up -d kafka-2'
    dag=dag,
)

# 3. Iniciar el Producer (después de Kafka)
start_producer = BashOperator(
    task_id='start_producer',
    bash_command='docker start diciembre27-producer-1',  # O 'docker-compose up -d producer'
    dag=dag,
)

# 4. Iniciar Spark (después de que el Producer esté activo)
start_spark = BashOperator(
    task_id='start_spark',
    bash_command='docker start diciembre27-spark-1',  # O 'docker-compose up -d spark'
    dag=dag,
)

# Definir las dependencias en el DAG
start_zookeeper >> [start_kafka_1, start_kafka_2] >> start_producer >> start_spark
