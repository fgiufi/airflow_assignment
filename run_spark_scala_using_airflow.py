from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


def print_massage():
    print("deal with orignal data!")

default_args={
    'owner':'datamaking',
    'start_date':datetime(2022,9,13),
    'retries':0,
    'catchup':False,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG('run_spark_scala_job',
          default_args=default_args,
          schedule_interval='* * * * *',
          )

python_operator = PythonOperator(task_id='print_massage_task',
                                python_callable=print_massage,
                                dag=dag)

LongestTimeSpan_spark_operator = BashOperator(
    task_id='LongestTimeSpan',
    dag=dag,
    bash_command="sh /Users/shiyu.chenthoughtworks.com/Documents/airflow_learning/bash_command/LongestTimeSpan.sh "
)

ProductSelect_spark_operator = BashOperator(
    task_id='ProductSelect',
    dag=dag,
    bash_command="sh /Users/shiyu.chenthoughtworks.com/Documents/airflow_learning/bash_command/ProductSelect.sh "
)

ProfitsTop10_spark_operator = BashOperator(
    task_id='ProfitsTop10',
    dag=dag,
    bash_command="sh /Users/shiyu.chenthoughtworks.com/Documents/airflow_learning/bash_command/ProfitsTop10.sh "
)

python_operator >> [LongestTimeSpan_spark_operator,ProductSelect_spark_operator,ProfitsTop10_spark_operator]
