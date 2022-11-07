# 1) Lets rewrite the code using the taskflow
# 2) The taskflow API will automatically calculate the dependencies
# 3) That is, the taskflow API takes care of the xcom's push and pull operations.
# 4) Here we include multiple outputs through the decorators

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

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Jerry', 'last_name': 'Freedman'}

    @task()
    def get_age():
        return 19

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name}"
        f"and I am {age} years old")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = hello_world_etl() #creating an instance of our dag

