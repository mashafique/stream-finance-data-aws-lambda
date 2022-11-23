import json
import boto3
import random
import datetime
import yfinance as yf
from time import sleep
import pandas as pd
import os

REGION = os.environ['REGION']
STREAM_NAME = os.environ['STREAM_NAME']

# kinesis = boto3.client('kinesis', "us-east-2")
kinesis = boto3.client('kinesis', REGION)

tickers=["FB", "SHOP", "BYND", "NFLX", "PINS", "SQ", "TTD", "OKTA", "SNAP", "DDOG"]

def getReferrer(i,ticker,yf_data):
    data = {}
    data['high'] = round(yf_data['High'][i], 2)
    data['low'] = round(yf_data['Low'][i], 2)
    data['ts'] = str(yf_data.High.index[i])
    data['name'] = ticker
    return data

def lambda_handler(event,context):
    for ticker in tickers:
        yf_data = yf.download(ticker, start="2022-05-02", end="2022-05-03", interval="5m")

        for i in range(len(yf_data)):
            data = json.dumps(getReferrer(i,ticker,yf_data))+"\n"
            print(data)
            output = kinesis.put_record(
                # StreamName="mohammad-project03",
                StreamName=STREAM_NAME,
                Data=data,
                PartitionKey="partitionkey")
            print(output)

    return {
        'statusCode': 200,
        'body': json.dumps('Done!')
    }
