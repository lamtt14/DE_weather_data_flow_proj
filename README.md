# OpenWeather data project

## Project Overview
This project is a weather data ETL pipeline using Apache Airflow, Apache Spark, Kafka, and PostgreSQL. It fetches weather data from the OpenWeatherMap API for multiple cities, ingests it into Kafka, processes it with Spark, and stores the results in a PostgreSQL database. The system is containerized using Docker Compose.

## Architecture & Data Flow

- **Airflow** (`airflow/dags/`):
  - `batch_weather_data.py`: Defines the main ETL DAG. Runs a Python task to fetch weather data and a Spark job to process it.
  - `weather_producer.py`: Contains `WeatherProducer`, which fetches weather data from the API and sends it to Kafka.
- **Spark** (`spark/`):
  - `main.py`: Consumes weather data from Kafka, transforms it, and writes to PostgreSQL. Handles offset checkpointing in `checkpoint/last_offsets.json`.
  - `transform.py`, `spark_utils.py`: Define schema and transformation logic for weather data.
- **Docker** (`docker-compose.yaml`):
  - Orchestrates Airflow, Spark, Kafka, PostgreSQL, and Redis containers. Mounts code and checkpoint files into containers.



