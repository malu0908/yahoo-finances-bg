import pandas as pd
import config
import boto3 as boto3
import s3fs
from datetime import datetime

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
  url = 'https://query1.finance.yahoo.com/v8/finance/chart/' + ticker + '?interval=1m'
  df_cot = pd.read_csv(url)
  df = pd.DataFrame(df_cot)

  bytes_to_write = df.to_csv(None).encode()
  with fs.open(f's3://yahoofinances-demo/inbound/{datetime.now().year}/{datetime.now().month}/{datetime.now().day}/finances_{ticker}_{datetime.now()}.csv', 'wb') as f:
    f.write(bytes_to_write)
