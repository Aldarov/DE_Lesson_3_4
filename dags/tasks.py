from datetime import datetime
import random
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def hello():
    print("Airflow")

def generate_numbers():
    num1 = random.randint(0, 100)
    num2 = random.randint(0, 100)
    with open("result.txt","w") as file:
        file.write(f"{num1} {num2}")

with DAG(dag_id="first_dag", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    hello_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task1 = PythonOperator(task_id="task1", python_callable = hello)
    generate_numbers_task = PythonOperator(task_id="generate_numbers_task", python_callable = hello)

    hello_task >> python_task1 >> generate_numbers_task