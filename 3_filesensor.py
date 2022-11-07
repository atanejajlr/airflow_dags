"""

About Sensors:

1) We are going to discover a very important topic in airflow - airflow sensors
2) What if you need to wait for a file to land at a specific location?
3) What if youy want to wait for a task in another FAG to complete before executing you DAG?
4) Or, what if you want to waut for a value to be n your SQL table before aexecuting the next task?
5) To answwe all this pretty important questions, you need an "Airflo sensor"

Here we kearn the following:

1) What is an "Airflow Sensor"
2) It's limitations
3) What ius a Deadlock?
4) Best practises around Sensor.

"""

"""

1) Lets u say we have 3 dfferent oparrtners
2) We ahve 3 different parners as shown
3) The 3 partners send us data at differnt times
4) Let us say Partner A sends us data at 09:00 AM, PArtner B at 09:30 AM and Partner C at 10:40 AM
5) Once we have received the data, we have to extract the data
6) And then process the data
7) How do you know the data is arrived?
8) This is where we need to use special kind of operators called sensors


"""

"""

1) What is a Sensor?
2) A Sensor is an operator evaluating at a given interval of time if  a condition is true or false
3) For example, if a file exists at a sp[ececific location it is true
4) So basically, each time you need something to happen before going to the next task you need a sensor
5) By deafault the condition is evaluated every 30 secs
6) You have : File Sensor, SQL Sensor
7) If you want to wait for a task in another DAG To complete ypu have External file sensor
8) there are several sensors
9) Implementing sensor
10) As it may be noticed that we have defined a poke interval of 30 seconds that isa airflow will check every 30 seconds 
if the file has arrived or not
11)If we haven't seta  timeout, we will get a deadlock i.e. no workers will be available to perform the task
12) To undersatnd better, refer: https://youtu.be/fgm3BZ3Ubnw?list=PL79i7SgJCJ9hu5GqcA091h6zuewmsvSyy

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.exceptions import AirflowSensorTimeout
from datetime import datetime

default_args = {

    'start_date': datetime(2022, 1, 1)

}

def _done():
    pass

def _partner_a():
    return False

def _partner_b():
    return True

def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
        print("Sensor timed out")
        with DAG('my_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
            partner_a = PythonSensor(
            task_id='partner_a',
            poke_interval=120,
            timeout=60 * 10,
            mode="reschedule",
            python_callable=_partner_a,
            on_failure_callback=_failure_callback,
            soft_fail=True
            )

            partner_b = PythonSensor(
            task_id='partner_b',
            poke_interval=120,
            timeout=60 * 10,
            mode="reschedule",
            python_callable=_partner_b,
            on_failure_callback=_failure_callback,
            soft_fail=True
            )
            done = PythonOperator(
            task_id="done",
            python_callable=_done,
            trigger_rule='none_failed_or_skipped'
            )
            [partner_a, partner_b] >> done



