from datetime import datetime
from airflow import DAG, Dataset
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

# Define the consumer DAG
with DAG(dag_id='consumer',
        start_date = datetime.now(),
        default_args = default_args) as dag:

    # Define the task that consumes the dataset
    consume_dataset = BashOperator(
        task_id="consume",
        bash_command="cat /tmp/data.csv",
        retries=3,
    )
