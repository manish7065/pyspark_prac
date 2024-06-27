from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False
}

dag = DAG('hive_spark_integration', default_args=default_args, schedule_interval='@daily')

hive_task = BashOperator(
    task_id='run_hive_query',
    bash_command='hive -e "SELECT * FROM sales WHERE year = 2023"',
    dag=dag
)

spark_task = BashOperator(
    task_id='run_spark_job',
    bash_command='spark-submit --class com.example.SparkJob /path/to/spark-job.jar',
    dag=dag
)

hive_task >> spark_task
