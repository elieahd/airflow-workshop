import datetime
import pendulum
import os
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

POSTGRES_CONN_ID = 'postgres-connection-id'

with DAG(
    dag_id = "process-employees",
    schedule_interval = "0 0 * * *",
    start_date = pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup = False,
    dagrun_timeout = datetime.timedelta(minutes=60)
) as dag:

    create_employees_table = PostgresOperator(
      task_id="create_employees_table",
      postgres_conn_id=POSTGRES_CONN_ID,
      sql="sql/create_employees_table.sql"
    )

    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/create_employees_temp_table.sql"
    )

    @task
    def get_data():
      data_path = "/opt/airflow/dags/files/employees.csv"
      os.makedirs(os.path.dirname(data_path), exist_ok=True)
      url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"
      response = requests.request("GET", url)
      with open(data_path, "w") as file:
        file.write(response.text)
      postgres_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
      conn = postgres_hook.get_conn()
      cur = conn.cursor()
      with open(data_path, "r") as file:
        cur.copy_expert(
          "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
          file,
        )
      conn.commit()

    @task
    def merge_data():
      query = """
          INSERT INTO employees
          SELECT *
          FROM (
              SELECT DISTINCT *
              FROM employees_temp
          ) t
          ON CONFLICT ("Serial Number") DO UPDATE
          SET "Serial Number" = excluded."Serial Number";
      """
      try:
        postgres_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return 0
      except Exception as e:
        return 1

    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()
