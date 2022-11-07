# 1) The purpose of this tutorial is to demonstrate what happend when cachup is set to false
# 2) DAG Runs older the current date are not executed 
# 3) When catchup is set to true, the older DAG runs also executed

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {

'owner': 'ataneja',
'retries': 5,
'retry_delay': timedelta(minutes=5)


}

with DAG(

    dag_id='dag_with_catchup',
    default_args=default_args,
    start_date=datetime(2022, 11, 7),
    schedule_interval='@daily',
    catchup=False

) as dag:


    task1 = BashOperator(

        task_id='task1',
        bash_command='echo this is a simple bash command!'
)

