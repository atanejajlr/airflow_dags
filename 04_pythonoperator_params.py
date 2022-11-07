#1) In this DAG we are using a Python operator with arguments
#2) Again steps in the tutorial are as follows:
# a) Import the modules
# b) Define the default_args as global variable. This is a dictionary and typically comprises of: ownwe, retries, retry_delay
# c) Define the python callable. This time with arguments
# d) Instantiate the dag object
# e) Define the operator. The operator should typically have: task_id, python callable, and other params. 
# Here since we are using parameters within the python callable, we ought to give a dictionary to it which 
# is defined in the pthon operator



from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {

    'owner': 'ataneja',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)

}

def greet(name, age):
    print("Hello world. You're now using the Python operator with parameters")
    print(f"my name is {name} and I am {age} years old")

with DAG(

    default_args=default_args,
    dag_id='dag_with_python_operator',
    description="this is the dag with python operator and parameters",
    start_date=datetime(2022, 11, 6),
    schedule='@daily'



) as dag:


    task1=PythonOperator(

        task_id='greet',
        python_callable=greet,
        op_kwargs={'name': 'Tom', 'age': 20}
    )

task1