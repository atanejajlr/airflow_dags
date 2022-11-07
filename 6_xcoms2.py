# This is the same as python file: 5_xcoms.py but we have the same task returning name and age

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {

    'owner': 'ataneja',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}

def greet(age, ti): #ti is the task instance
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name') #we have to specify which key to pull. Data exchange between the tasks happens here
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name') #we have to specify which key to pull. Data exchange between the tasks happens here
    print("Hello world. You're now using the Python operator with xcom and ketys")
    print(f"my first name is {first_name} and my last name is {last_name} and my age is {age}")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

with DAG(

    default_args=default_args,
    dag_id='dag_with_python_operator and x_com',
    description="this is the dag with python operator and x_com",
    start_date=datetime(2022, 11, 6),
    schedule='@daily'



) as dag:

    task1=PythonOperator(

    task_id='greet',
    python_callable=greet,
    op_kwargs={'age': 20}

    )


    task2=PythonOperator(

        task_id='get_name',
        python_callable=get_name,
    )

task2 >> task1

