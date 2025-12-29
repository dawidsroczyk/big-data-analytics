from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="weather_preprocessing",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_spark_weather_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.WeatherPreprocessingJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_spark_job