import random
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


path_to_file = os.path.join(os.path.dirname(__file__), 'res.txt')

def hello():
    print("Airflow!!!")

def add_numbers_to_file():
    num1 = random.randint(0, 100)
    num2 = random.randint(0, 100)
    line = f"{num1} {num2}\n"
    delete_last_line_file()
    with open(path_to_file, "a", encoding = 'utf-8') as file:
        file.write(line)

def delete_last_line_file():
    if os.path.isfile(path_to_file):
        with open(path_to_file, "rb+") as file:
            try:
                file.seek(-2, os.SEEK_END)
                while file.read(1) != b'\n':
                    file.seek(-2, os.SEEK_CUR)
            except OSError:
                file.seek(0)
            pos = file.tell()
            last_line = file.readline().decode().split(' ')
            if len(last_line) < 2:
                file.truncate(pos)

def result_calculation():
    delete_last_line_file()
    result = 0
    with open(path_to_file) as file:
        sum_col_1 = 0
        sum_col_2 = 0
        for line in file:
            digits = line.split(' ')
            sum_col_1 += int(digits[0])
            sum_col_2 += int(digits[1])
        result = sum_col_1 - sum_col_2

    with open(path_to_file, "a", encoding = 'utf-8') as file:
        file.write(str(result)+"\n")

with DAG(dag_id="first_dag", start_date=datetime(2022, 1, 1), schedule="20-25 16 * * *") as dag:
    hello_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task1 = PythonOperator(task_id="task1", python_callable = hello)
    add_numbers_to_file = PythonOperator(task_id="add_numbers_to_file", python_callable = add_numbers_to_file)
    result_calculation = PythonOperator(task_id="result_calculation", python_callable = result_calculation)

    hello_task >> python_task1 >> add_numbers_to_file >> result_calculation
