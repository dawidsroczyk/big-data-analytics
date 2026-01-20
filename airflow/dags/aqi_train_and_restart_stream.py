from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="aqi_train_and_restart_stream",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    train = BashOperator(
        task_id="train_model",
        bash_command="""
        docker exec spark-master /spark/bin/spark-submit \
          --class preprocessing.job.AqiModelTrainingJob \
          --master spark://spark-master:7077 \
          /spark/jars/preprocessing-module-assembly-0.1.0-SNAPSHOT.jar
        """
    )

    stop_stream = BashOperator(
        task_id="stop_stream",
        bash_command=r"""
        docker exec spark-master bash -lc '
          set -euo pipefail

          pids=$(pgrep -f "spark-submit.*gold_features_to_mongo\.py" || true)

          if [ -n "$pids" ]; then
            echo "Stopping stream PIDs: $pids"
            kill -TERM $pids || true
            sleep 10

            still=$(pgrep -f "spark-submit.*gold_features_to_mongo\.py" || true)
            if [ -n "$still" ]; then
              echo "Still running, forcing kill: $still"
              kill -KILL $still || true
            fi
          else
            echo "No stream process found"
          fi
        '
        """
    )


    start_stream = BashOperator(
        task_id="start_stream",
        bash_command=r"""
        docker exec -d spark-master bash -lc '
          nohup /spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /spark/jobs/gold_features_to_mongo.py \
            > /tmp/gold_stream.out 2>&1 &
        '
        """
    )

    train >> stop_stream >> start_stream
