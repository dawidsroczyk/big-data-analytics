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
          
          TARGET="gold_features_to_mongo\.py"

          pids=$(pgrep -f "$TARGET" || true)

          if [ -n "$pids" ]; then
            echo "Found running stream with PIDs: $pids. Stopping..."
            kill -TERM $pids || true
            sleep 10

            still=$(pgrep -f "$TARGET" || true)
            if [ -n "$still" ]; then
              echo "Stream still running, forcing kill: $still"
              kill -KILL $still || true
            fi
          else
            echo "No existing stream process found (safe to proceed)"
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
            --driver-memory 4G \
            --executor-memory 4G \
            --conf spark.cores.max=4 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
            /spark/jobs/gold_features_to_mongo.py \
            > /tmp/gold_stream.out 2>&1 &
        '
        """
    )

    train >> stop_stream >> start_stream