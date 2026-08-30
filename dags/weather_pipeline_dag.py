from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/opt/airflow/src')

from load_weather_data_to_bronze import load_weather_data_to_bronze
from transform_weather_data_silver import transform_weather_data_to_silver
from transform_weather_data_month_year_gold import transform_weather_to_gold

default_args = {
    "owner": "guilherme",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="0 6 5 * *",
    catchup=False,
    tags=["weather", "portfolio"],
) as dag:

    bronze_task = PythonOperator(
        task_id="load_bronze",
        python_callable=load_weather_data_to_bronze,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    silver_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_weather_data_to_silver,
    )

    gold_task = PythonOperator(
        task_id="transform_gold",
        python_callable=transform_weather_to_gold,
    )

    bronze_task >> silver_task >> gold_task