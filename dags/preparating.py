from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="preparating",
    start_date=datetime(2022, 6, 20),
    schedule_interval="0 2 * * *",
    catchup = False
) as dag:
    preparating_task = BashOperator(
        task_id='preparating_task',
        bash_command='python /home/bootcamp/Documents/airflow-on-docker-main/airflow/plugins/house_prediction/preparating.py'
    )


preparating_task
