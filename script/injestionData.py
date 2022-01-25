import pandas as pd
import config
import boto3
from datetime import datetime
from urllib.request import urlopen
import json

tickers = [
  'AAPL',
  'VALE',
]

#Creating Session With Boto3.
session = boto3.Session(
  aws_access_key_id=config.aws_access_key_id,
  aws_secret_access_key=config.aws_secret_access_key
)
s3 = boto3.resource( 's3', aws_access_key_id=config.aws_access_key_id, aws_secret_access_key=config.aws_secret_access_key)
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
