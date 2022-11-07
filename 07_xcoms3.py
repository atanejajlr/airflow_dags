# 1) This is the same as 6_xcoms2.py
# 2) Except that we pass everything through xcoms
# 3) Including the age parameter
# 4) Where we have 3 values that are pushed and pulled successfully
# 5) Although, it is very handy to push and pull through xcoms, the maximum size of xcoms is 48KB.
# 6) YEs, only 48KB
# 7) So never use xcoms to use large data especially pandas dataframe

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
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print("Hello world. You're now using the Python operator with xcom and ketys")
    print(f"my first name is {first_name} and my last name is {last_name} and my age is {age}")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')

def get_age(ti):

    ti.xcom_push(key='age', value=19)

with DAG(

    default_args=default_args,
    dag_id='dag_with_python_operator and x_com, get everything through xcoms',
    description="this is the dag with python operator and x_com, get everything through xcoms",
    start_date=datetime(2022, 11, 6),
    schedule='@daily'



) as dag:

    task1=PythonOperator(

    task_id='greet',
    python_callable=greet,
    #op_kwargs={'age': 20}

    )


    task2=PythonOperator(

        task_id='get_name',
        python_callable=get_name,
    )

    
    task3=PythonOperator(

        task_id='get_age',
        python_callable=get_age,
    )

[task2, task3] >> task1
