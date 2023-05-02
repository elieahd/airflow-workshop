from datetime import datetime, timedelta
from airflow import DAG, Dataset
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(dag_id='producer',
        start_date = datetime.now(),
        schedule_interval = timedelta(minutes=5),
        default_args=default_args) as dag:

    generate_dataset = BashOperator(
        task_id='generate_dataset',
        bash_command='echo "data1,data2,data3\n" >> /tmp/data.csv',
    )

    check_file_task = BashOperator(
        task_id='check_file',
        bash_command='cat /tmp/data.csv',
    )

    generate_dataset >> check_file_task

