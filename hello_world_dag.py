from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def say_hello():
    print("Hello from Airflow on KubernetesExecutor!")

with DAG(dag_id="hello_world",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@once",
         catchup=False) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=say_hello
    )

