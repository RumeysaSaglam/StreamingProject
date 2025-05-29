from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

import sys
import os
scripts_path = os.path.join(os.path.dirname(__file__), '..', 'scripts')
sys.path.append(os.path.abspath(scripts_path))

from kafka_streaming_service import initiate_stream  

# Default arguments
DAG_START_DATE = datetime(2025, 5, 20, 12, 0)

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': DAG_START_DATE,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='name_stream_dag',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval='0 1 * * *',
    catchup=False,
    description='Stream random names to Kafka topic and process with Spark',
    max_active_runs=1,
    tags=['streaming', 'kafka', 'spark']
) as dag:
    
    # Check dependencies
    check_services = BashOperator(
        task_id='check_services',
        bash_command='''
        echo "Checking Kafka broker..."
        nc -zv kafka_broker 19092 || exit 1
        
        echo "Checking Spark master..."
        nc -zv spark_master 7077 || exit 1
        
        echo "All services are ready"
        ''',
    )
    
    # Kafka streaming task
    kafka_stream_task = PythonOperator(
        task_id='stream_to_kafka_task', 
        python_callable=initiate_stream
    )

    # Spark submit task - Docker Compose içinde çalışacak şekilde
    spark_submit_task = SparkSubmitOperator(
        task_id='spark_processing_task',
        application='/opt/airflow/scripts/spark_processing.py',
        name='kafka-postgres-streaming',
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark_master:7077',
            'spark.hadoop.security.authentication': 'simple',
            'spark.hadoop.security.authorization': 'false',
            'spark.sql.warehouse.dir': '/tmp/spark-warehouse',
            'spark.jars.ivy': '/tmp/.ivy2'
        },
        packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0',
        jars='/opt/airflow/jars/postgresql-42.7.4.jar',
        executor_memory='2g',
        driver_memory='2g',
        env_vars={
            'USER': 'sparkuser',
            'HADOOP_USER_NAME': 'sparkuser',
            'SPARK_USER': 'sparkuser',
            'HOME': '/tmp'
        }
    )

    # Task dependencies
    check_services >> kafka_stream_task >> spark_submit_task