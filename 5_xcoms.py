# 1) In this file we demonstrate how to share information between different tasks.
# 2) This can be achieved through Airflow xcoms
# 3) Basically we can push information to xcoms in one task and pull information in other tasks.
# 4) By default, every function's return value will be automatically pushed into xcoms
# 5) We we will create a new Python function now with a task instance ti
# 6) See below what is done, here, we have 2 Python operators one associated with Python callable 
# with as task instance returning the name and another which returns the age


from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {

    'owner': 'ataneja',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}

def greet(age, ti): #ti is the task instance
    name = ti.xcom_pull(task_ids='get_name') # exchanging data between tasks
    print("Hello world. You're now using the Python operator with parameters")
    print(f"my name is {name} and I am {age} years old")

def get_name():
    return "Jerry"

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

