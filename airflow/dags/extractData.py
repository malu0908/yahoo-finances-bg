# [START import_module]
from airflow.models import DAG
import pandas as pd
import boto3
from datetime import datetime
from urllib.request import urlopen
import os
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
# [END import_module]

# [START default_args]
default_args = {
  'owner': 'Otávio Faria',
  'depends_on_past': False,
  'email': ['otavio.faria@alphabot.com.br'],
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START env_variables]
ACCESS_KEY = os.getenv("ACCESS_KEY", "YOURACCESSKEY")
SECRET_ACCESS = os.getenv("SECRET_KEY", "YOURSECRETKEY")
# [END env_variables]

# [START instantiate_dag]
dag = DAG(
  'extract-data',
  default_args=default_args,
  start_date=datetime(2022, 1, 24),
  schedule_interval='@day',
  tags=['extract', 'inbound', 'S3']
)
# [END instantiate_dag]

# [START functions]
def get_data_yahoo_finances():
  print('a')
# [END functions]

extract_data_yahoo_finances = PythonOperator(
	task_id='extract_data_yahoo_finances',
	python_callable=get_data_yahoo_finances,
	dag=dag
)

[extract_data_yahoo_finances]
