# 1) Lets rewrite the code using the taskflow
# 2) The taskflow API will automatically calculate the dependencies
# 3) That is, the taskflow API takes care of the xcom's push and pull operations.

from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
'owner': 'ataneja',
'retries': 5,
'retry_delay': timedelta(minutes=5)


}

@dag(dag_id='dag_with_taskflow_api',
    default_args=default_args,
    start_date=datetime(2022, 11, 7),
    schedule_interval='@daily')

def hello_world_etl():

    @task()
    def get_name():
        return "Jerry"

    @task()
    def get_age():
        return 19

    @task()
    def greet(name, age):
        print(f"Hello World! My name is {name}"
        f"and I am {age} years old")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hello_world_etl() #creating an instance of our dag



    




