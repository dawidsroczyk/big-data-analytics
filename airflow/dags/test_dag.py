from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
):
    t1 = BashOperator(
        task_id="hello",
        bash_command="echo 'Airflow is running!'"
    )