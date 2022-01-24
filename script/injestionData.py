import pandas as pd
import config
import boto3 as boto3
import s3fs
from datetime import datetime
from urllib.request import urlopen
import json

tickers = [
  'KLBN11.SA',
  'RENT3.SA',
  'EQTL3.SA',
  'ECOR3.SA',
  'BRML3.SA',
  'SANB11.SA',
  'LREN3.SA',
  'MULT3.SA',
  'GOAU4.SA',
  'USIM5.SA',
  '^BVSP'
]
  
fs = s3fs.S3FileSystem(key=config.aws_access_key_id, secret=config.aws_secret_access_key)
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
  with fs.open(f's3://yahoofinances-demo/inbound/{datetime.now().year}/{datetime.now().month}/{ticker}/finances_{ticker}_{datetime.now().day}.csv', 'wb') as f:
    f.write(bytes_to_write)
