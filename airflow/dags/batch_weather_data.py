# dags/weather_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from weather_producer import WeatherProducer
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = ["Hanoi", "Ho Chi Minh City", "HaiPhong"]


def run_weather_batch():
    producer = WeatherProducer(API_KEY)
    producer.run_batch(CITIES, delay_between_cities=5)


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "my_weather_flow_dag",
    default_args=default_args,
    description="ETL pipeline",
    # schedule="*/15 * * * *",  # mỗi 15 min
    schedule=None,
    start_date=datetime(2025, 9, 20),
    catchup=False,
    tags=["weather", "batch", "kafka"],
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=run_weather_batch,
    )

    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_consumer_job",
        application="/opt/spark-apps/main.py",
        conn_id="spark_default",  # Connection đã tạo trong UI
        deploy_mode="client",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.7.3",
        verbose=True,
    )

    fetch_weather >> run_spark_job
