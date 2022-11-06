"""
1) Let's create a simple DAG with bashoperator
2) In airflow, an airflow DAG is defined as a Python file
3) The DAG implementation is the instantiation of the class DAG
4) Therefore, we have to firstly import the DAG From airflow
5) Then, we will create an instance of DAG using the with statement


"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {

    'owner': 'ajay',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)


}
with DAG(


    dag_id='our_first_dag',
    description='this is the first dag that we are writing',
    start_date=datetime(2022, 7, 29, 2),
    schedule_interval='@daily'

) as dag:
    task1 = BashOperator(

        task_id= 'first_task',
        bash_command="echo hello world , this is the first task"



    )
     


