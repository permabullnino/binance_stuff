from datetime import date, datetime, time, timedelta
from binance.client import Client
import pandas as pd
import numpy as np

class binance_add_metrics():

    def __init__(self):
        self.coin = 'BTCUSDT'
        self.coin1 = 'ETHUSDT'
        self.date = '10 Sep, 2019'
        self.date1 = '30 Dec, 2021'
        self.timestamp = 1569888000
        self.timestamp1 = 1576137600001
        self.timestamp2 = 1604937600002

    def metric_price(self, asset):
        # Price Data
        df = pd.DataFrame(Client().get_historical_klines(asset, Client.KLINE_INTERVAL_8HOUR, self.timestamp, self.date1))
        df.columns = ['fundingTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close Time', 'Quote Asset Volume', 'Number of Trades', 'Taker Buy Base Asset Volume', 'Taker Buy Quote Asset Volume', 'Ignore']

        df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')    #format date
        df['Open'] = pd.to_numeric(df['Open'])

        print(df)

        return df

    def metric_funding(self, asset):
        # There's a 1000 call limit on the funding data, so need to pull in multiple datasets then merge

        # Funding Data
        df1 = Client().futures_funding_rate(symbol=asset, startTime=self.timestamp, endTime=self.timestamp1, limit=1000)    #initial launch dataframe
        df1 = pd.DataFrame(df1)
        df1['fundingTime'] = pd.to_datetime(df1['fundingTime'], unit='ms')
        df1['fundingRate'] = pd.to_numeric(df1['fundingRate'])
        
        df2 = Client().futures_funding_rate(symbol=asset, startTime=self.timestamp1, endTime=self.timestamp2, limit=1000)
        df2 = pd.DataFrame(df2)
        df2['fundingTime'] = pd.to_datetime(df2['fundingTime'], unit='ms')
        df2['fundingRate'] = pd.to_numeric(df2['fundingRate'])

        df3 = Client().futures_funding_rate(symbol=asset, startTime=self.timestamp2, limit=1000)    #newest dataframe
        df3 = pd.DataFrame(df3)
        df3['fundingTime'] = pd.to_datetime(df3['fundingTime'], unit='ms')
        df3['fundingRate'] = pd.to_numeric(df3['fundingRate'])

        dflist = [df1,df2,df3]
        dfin = pd.concat(dflist)
        
        dfin['fundingRate_pos'] = np.where(dfin['fundingRate'] >= 0, dfin['fundingRate'],0)     #separate positive funding values into new column
        dfin['fundingRate_neg'] = np.where(dfin['fundingRate'] <= 0, dfin['fundingRate'],0)     #separate positive funding values into new column

        print(dfin)

        return dfin

    def metric_funding_change(self, asset, period):
        dfin = self.metric_funding(asset)

        dfin['fundingChange'] = dfin['fundingRate'].diff(period)
        dfin['fundingChange_pos'] = np.where(dfin['fundingChange'] >= 0, dfin['fundingChange'],0)
        dfin['fundingChange_neg'] = np.where(dfin['fundingChange'] <= 0, dfin['fundingChange'],0)

        return dfin