# -*- coding: utf-8 -*-

# 在binance API 现货交易查询2023-01-01至2023-03-15 BTC 4小时级别
#（一根k线代表4小时）的交易数据，开盘/收盘/最高价/最低价/交易量，并计算
# 当价格有效涨/跌破MA20 （20根k线柱-4小时一根）的24小时内最大涨跌幅及收盘涨跌幅。

# 1，MA20 计算方式可查询google 
# 2，有效涨跌的定义：当价格涨/跌破MA20 后的24 小时内收盘价没有回到MA20 下方/上方
# 3，API 文档 https://binance-docs.github.io/apidocs/spot/en/#spot-account-trade

# 任务：
# 1，获取该时段内开盘/收盘/最高价/最低价/交易量 五个指标的数据
# 2，统计时间段内共有多少次涨破或跌破4小时级别MA20，有多少次有效涨破或跌破
# 3，计算每次有效涨/跌破MA20后的最大涨跌幅及收盘涨跌幅

import requests
import pandas as pd
import numpy as np
from datetime import datetime
'''
############# This part doesn't work temporarly. I used below to connect data on binance before. ############# 
# web scarping via Binance API
# endpoint for klines
url = 'https://api.binance.com/api/v3/klines'

# parameters for klines
symbol = 'BTCUSDT'
interval = '4h'
startTime = '2023-01-01 00:00:00'
endTime = '2023-03-15 23:59:59'

# convert start and end time to timestamp
startTimestamp = int(pd.Timestamp(startTime).timestamp() * 1000)
endTimestamp = int(pd.Timestamp(endTime).timestamp() * 1000)
# send request

params = {
    'symbol': symbol,
    'interval': interval,
    'startTime': startTimestamp,
    'endTime': endTimestamp,
    'limit': 1000  # max limit
}
response = requests.get(url, params=params)

# parse response
data = response.json()
df = pd.DataFrame(data, columns=['openTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime', 'quoteAssetVolume',
                                 'numberOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume', 'ignore'])

# convert columns to numeric
df = df.apply(pd.to_numeric)
df['openTime'] = pd.to_datetime(df['openTime'], unit='ms')

# set openTime as index and drop unnecessary columns
df.set_index('openTime', inplace=True)
df.drop(['closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseAssetVolume', 'takerBuyQuoteAssetVolume',
         'ignore'], axis=1, inplace=True)

# calculate MA20
df['MA20'] = df['close'].rolling(20).mean()

print(df)
'''

def isValidChange(df): 
  # df: dataframe
  # return: a list of result
  # NA: data is not sufficient to judge whether it is a valid change
  # 1 : is a positive valid change
  # -1: is a negative valid change
  rows = df.shape[0]
  res = []
  for i in range(rows):
    # use a flag to store current result
    flag = pd.NA
    if pd.isna(df.iloc[i]['MA20']):
      res.append(flag)
      continue
    else:
    # First: judge if the price rise over or drop below the MA20 line
      if (df.iloc[i]['open'] <= df.iloc[i]['MA20'] and df.iloc[i]['close'] <= df.iloc[i]['MA20']) or\
        (df.iloc[i]['open'] >= df.iloc[i]['MA20'] and df.iloc[i]['close'] >= df.iloc[i]['MA20']):
        res.append(flag)
        continue
      # if the price rise over the MA20 line:
      elif df.iloc[i]['open'] < df.iloc[i]['MA20'] and df.iloc[i]['close'] > df.iloc[i]['MA20']:
        flag = 1
        # search if it drops below MA20 line in the next 24h
        for j in range(1,7):
          if i+j >= rows:
            flag = pd.NA
            break
          if df.iloc[i]['MA20'] > df.iloc[i+j]['close']:
            flag = pd.NA
            break
        res.append(flag)
        continue
      # if the price drop below the MA20 line:
      elif df.iloc[i]['open'] > df.iloc[i]['MA20'] and df.iloc[i]['close'] < df.iloc[i]['MA20']:
        flag = -1
        for j in range(1,7):
          # search if it drops below MA20 line in the next 24h
          if i + j >= rows:
            flag = pd.NA
            break
          if df.iloc[i]['MA20'] < df.iloc[i+j]['close']:
            flag = pd.NA
            break
        res.append(flag)
        continue
  return res

def MaxChangeRate(df):
  # df: data
  # return res: a list, in which find the max rate of rising/dropping, other
  # rows that are not valid change will be filled by NA
  res = []
  rows = df.shape[0]
  for i in range(rows):
    if pd.isna(df.iloc[i]['isValidChange']):
      res.append(pd.NA)
    elif df.iloc[i]['isValidChange'] == 1:
      res.append(df.iloc[i:i+7]['ChangeRate'].max())
    elif df.iloc[i]['isValidChange'] == -1:
      res.append(df.iloc[i:i+7]['ChangeRate'].min())
    else:
      res.append(pd.NA)
  return res

# Get data
df = pd.read_csv('Binance0301_0315.csv')

# Calculate MA20
df['MA20'] = df['close'].rolling(20).mean()

# Identify valid changes
df['isValidChange'] = isValidChange(df)

# count all valid changes
print(f"The # of valid change is:{df['isValidChange'].value_counts()[1] + df['isValidChange'].value_counts()[-1]}")

# compute Change Rate
df['ChangeRate'] = (df['close'] - df['open']) / df['open'] * 100

# get the max change rate of rising/dropping
df['MaxValidChangeRate'] = MaxChangeRate(df)

# output file
df.to_csv("output.csv",index=False)