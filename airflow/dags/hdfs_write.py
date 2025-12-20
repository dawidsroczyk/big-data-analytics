from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "hdfs_write",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
):
    write = BashOperator(
        task_id="write_test",
        bash_command="""
        hdfs dfs -mkdir -p /airflow_test &&
        echo 'Hello from Airflow!' | hdfs dfs -put - /airflow_test/hello.txt
        """
    )