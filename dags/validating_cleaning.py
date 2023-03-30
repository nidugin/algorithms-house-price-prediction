from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="validating_cleaning",
    start_date=datetime(2022, 6, 20),
    schedule_interval="0 2 * * *",
    catchup = False
) as dag:
    validating_task = BashOperator(
        task_id='validating_task',
        bash_command='python /home/bootcamp/Documents/airflow-on-docker-main/airflow/plugins/house_prediction/validating.py'
    )
    
    cleaning_task = BashOperator(
        task_id='cleaning_task',
        bash_command='python /home/bootcamp/Documents/airflow-on-docker-main/airflow/plugins/house_prediction/cleaning.py'
    )


validating_task >> cleaning_task
