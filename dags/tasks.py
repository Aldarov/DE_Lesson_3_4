import random
import os
import psycopg2
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


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

def get_result_from_file():
    with open(path_to_file, "rb+") as file:
        try:
            file.seek(-2, os.SEEK_END)
            while file.read(1) != b'\n':
                file.seek(-2, os.SEEK_CUR)
        except OSError:
            file.seek(0)
        last_line = file.readline().decode().split(' ')
        if len(last_line) < 2:
            return int(last_line[0])
        return 0

def calc_result():
    result = 0
    with open(path_to_file) as file:
        lines = file.readlines()[:-1]
        sum_col_1 = 0
        sum_col_2 = 0
        for line in lines:
            digits = line.split(' ')
            sum_col_1 += int(digits[0])
            sum_col_2 += int(digits[1])
        result = sum_col_1 - sum_col_2
    return result

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

def check_file():
    if os.path.isfile(path_to_file) and calc_result() == get_result_from_file():
        return True

    return False

def branch_check_file():
    if os.path.isfile(path_to_file) and os.path.getsize(path_to_file) > 0:
        return "create_db"
    return "task_to_fail"

def task_to_fail():
    raise AirflowFailException("Входной файл пуст или его не существует")

def get_connection():
    conn_name = Variable.get("conn_name")
    print("conn_name: " + conn_name)
    return BaseHook.get_connection(conn_name)

def create_db():
    connection = get_connection()
    conn = psycopg2.connect(host=connection.host, port=connection.port, user=connection.login, password=connection.password, database=connection.schema)
    cursor = conn.cursor()
    cursor.execute("create table if not exists data(value_1 integer NOT NULL, value_2 integer NOT NULL)")
    with open(path_to_file) as file:
        lines = file.readlines()[:-1]
        for line in lines:
            digits = line.split(' ')
            value_1 = int(digits[0])
            value_2 = int(digits[1])
            cursor.execute(f"insert into data(value_1, value_2) values({value_1}, {value_2})")
    conn.commit()

    cursor.close()
    conn.close()

def add_result_to_db():
    connection = get_connection()
    conn = psycopg2.connect(host=connection.host, port=connection.port, user=connection.login, password=connection.password, database=connection.schema)
    cursor = conn.cursor()
    cursor.execute("delete from data")
    cursor.execute("alter table data ADD COLUMN IF NOT EXISTS coef integer;")
    sum_col_1 = 0
    sum_col_2 = 0
    with open(path_to_file) as file:
        lines = file.readlines()[:-1]
        for line in lines:
            digits = line.split(' ')
            value_1 = int(digits[0])
            value_2 = int(digits[1])
            sum_col_1 += value_1
            sum_col_2 += value_2
            coef = sum_col_1 - sum_col_2
            cursor.execute(f"insert into data(value_1, value_2, coef) values({value_1}, {value_2}, {coef})")
    conn.commit()

    cursor.close()
    conn.close()


with DAG(dag_id="first_dag", start_date=datetime(2022, 12, 11), schedule="0-5 3 * * *", catchup=False, max_active_runs=1) as dag:
    hello_task = BashOperator(task_id="hello", bash_command="echo hello")
    python_task1 = PythonOperator(task_id="task1", python_callable = hello)
    add_numbers_to_file = PythonOperator(task_id="add_numbers_to_file", python_callable = add_numbers_to_file, depends_on_past=True)
    result_calculation = PythonOperator(task_id="result_calculation", python_callable = result_calculation, depends_on_past=True)
    check_file = PythonSensor(task_id="check_file", python_callable=check_file)
    branch_check_file = BranchPythonOperator(task_id='branch_check_file', python_callable=branch_check_file)
    task_to_fail = PythonOperator(task_id="task_to_fail", python_callable = task_to_fail)
    create_db = PythonOperator(task_id="create_db", python_callable = create_db, depends_on_past=True)
    add_result_to_db = PythonOperator(task_id="add_result_to_db", python_callable = add_result_to_db, trigger_rule='none_failed_or_skipped', depends_on_past=True)

    hello_task >> python_task1 >> add_numbers_to_file >> result_calculation >> check_file >> branch_check_file >> [create_db, task_to_fail] >> add_result_to_db
