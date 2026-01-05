from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="combined_preprocessing",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    run_air_quality = BashOperator(
        task_id="air_quality_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.AirQualityPreprocessingJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_traffic = BashOperator(
        task_id="traffic_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.TrafficPreprocessingJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_uv = BashOperator(
        task_id="uv_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.UvPreprocessingJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_weather = BashOperator(
        task_id="weather_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.WeatherPreprocessingJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_full_preprocessing = BashOperator(
        task_id="full_preprocessing",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.FullPreproSafe \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_silver_hive = BashOperator(
        task_id="silver_hive",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.SilverHiveJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    run_gold_hive = BashOperator(
        task_id="gold_hive",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
            --class preprocessing.job.GoldHiveJob \
            --master spark://spark-master:7077 \
            /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    [run_air_quality, run_traffic, run_uv, run_weather] >> run_full_preprocessing >> run_silver_hive >> run_gold_hive
