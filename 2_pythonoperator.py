# This is the 3rd tutorial
# Here we use a Python operator
# Steps in the tutorial
#1) Import modules
#2) Define default_args as a global variable - this is a dictionary
#3) Define the DAG (or, instantiate the DAG object)
#4) Define  python callable
#5) Define the Python operator
#6) Define the task order. In this tutorial, there is only 1 task, yet, you ahve to define the task order.

#Your DAG is ready!


from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {

    'owner': 'ataneja',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}

def greet():
    print("Hello worls. You're now using the Python operator")

with DAG(

    default_args=default_args,
    dag_id='dag_with_python_operator',
    description="this is the 1st dag with python operator",
    start_date=datetime(2022, 11, 6),
    schedule='@daily'



) as dag:


    task1=PythonOperator(

        task_id='greet',
        python_callable=greet
    )

task1