#1) In airflow, creating a dag needs the schdule intreval parameter, which recerives a cron expression as a string
#or a datatime timedelta object
#2) What is a cron expression? A cron expression is a string comprising five fields separated by white space that
# rpresentsa  set of times normally as a schedule to execute some routine
#3) Preset cron expressions in airflow: None, @once, @hourly, @daily, @weekly, @monthly, @yearly

from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {

    'owner': 'ataneja',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(

    default_args=default_args,
    dag_id="dag_with_cron_expression",
    start_date=datetime(2022, 11, 1),
    schedule_interval='@daily'


    


) as dag:


    task1 = BashOperator(
    task_id='task1',
    bash_command="echo dag with cron expression"

    )


