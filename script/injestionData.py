from pandas_datareader import data as web
import pandas as pd
import config

ticker = '^BVSP'

start = pd.to_datetime(['2007-01-01']).astype(int)[0]//10**9 # convert to unix timestamp.
end = pd.to_datetime(['2020-12-31']).astype(int)[0]//10**9 # convert to unix timestamp.
url = 'https://query1.finance.yahoo.com/v7/finance/download/' + ticker + '?period1=' + str(start) + '&period2=' + str(end) + '&interval=1d&events=history'
df_cot = pd.read_csv(url)

display(df_cot)

pip install boto3

import boto3 as boto3 

session = boto3.Session(aws_access_key_id=config.aws_access_key_id, aws_secret_access_key=config.aws_secret_access_key)

s3 = session.resource('s3')

df_cot.to_csv('finances.csv')

s3.Bucket('yahoofinances-demo').upload_file('finances.csv','finances.csv')