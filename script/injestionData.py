import pandas as pd
import config
import boto3 as boto3

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

session = boto3.Session(aws_access_key_id=config.aws_access_key_id, aws_secret_access_key=config.aws_secret_access_key)
s3 = session.resource('s3')

for ticker in tickers:
  start = pd.to_datetime(['2007-01-01']).astype(int)[0]//10**9 # convert to unix timestamp.
  end = pd.to_datetime(['2020-12-31']).astype(int)[0]//10**9 # convert to unix timestamp.
  url = 'https://query1.finance.yahoo.com/v7/finance/download/' + ticker + '?period1=' + str(start) + '&period2=' + str(end) + '&interval=1d&events=history'
  df_cot = pd.read_csv(url)

  df_cot.to_csv(f'finances_{ticker}_{start}_{end}.csv')

# s3.Bucket('yahoofinances-demo').upload_file(f'finances_{ticker}_{start}_{end}.csv', f'finances_{ticker}_{start}_{end}.csv')
