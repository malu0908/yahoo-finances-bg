# [START import_module]
from airflow.models import DAG
import pandas as pd
import boto3
from datetime import datetime
from urllib.request import urlopen
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
ACCESS_KEY = getenv("ACCESS_KEY", "YOURACCESSKEY")
SECRET_ACCESS = getenv("SECRET_KEY", "YOURSECRETKEY")
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
	tickers = [
	  'AAPL',
		'VALE',
	]
		
	bucket_name = "yahoo-finances-bg"

	session = boto3.Session(
		aws_access_key_id=ACCESS_KEY,
		aws_secret_access_key=SECRET_ACCESS
	)
	s3 = boto3.resource( 's3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_ACCESS)
	bucket_name = "yahoo-finances-bg"

	for ticker in tickers:
		url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + ticker + '?interval=1h'
		response = urlopen(url)

		data_json = json.loads(response.read())
		data_timestamp = data_json['chart']['result'][0]['timestamp']
		data_json = data_json['chart']['result'][0]['indicators']['quote']
		df = pd.DataFrame.from_dict(data_json[0])
		df['timestamp'] = data_timestamp
		df['ticker'] = ticker
		df['year'] = datetime.now().year
		df['month'] = datetime.now().month
		df['day'] = datetime.now().day

		bytes_to_write = df.to_csv(None, index=False).encode()
		folder_name = f'inbound/{datetime.now().year}/{datetime.now().month}/{ticker}'
		s3.Object(bucket_name=bucket_name, key=f'{folder_name}/finances_{ticker}_{datetime.now().day}.csv').put(Body=bytes_to_write)
# [END functions]

extract_data_yahoo_finances = PythonOperator(
	task_id='extract_data_yahoo_finances',
	python_callable=get_data_yahoo_finances,
	dag=dag
)
	
[extract_data_yahoo_finances]
