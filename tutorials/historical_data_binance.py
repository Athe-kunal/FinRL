import requests
import json
import pandas as pd
from datetime import datetime,timedelta

class get_binance_data:
    def __init__(self,symbol,interval,start_time,end_time):
        self.url = "https://api.binance.com/api/v3/klines"

        startTime = datetime.strptime(start_time, '%Y-%m-%d')
        endTime = datetime.strptime(end_time, '%Y-%m-%d')
        
        self.start_time = str(int(startTime.timestamp() * 1000))
        self.end_time = str(int(endTime.timestamp() * 1000))
        # print(self.start_time,self.end_time)
        self.symbol = symbol
        self.interval = interval
        self.limit = '1000'

        
    def get_binance_bars(self,last_datetime):
        req_params = {"symbol": self.symbol, 'interval': self.interval,
                      'startTime': last_datetime, 'endTime': self.end_time, 'limit': self.limit}
        df = pd.DataFrame(json.loads(requests.get(self.url, params=req_params).text))
        print(df)
        if (len(df.index) == 0):
            return None
        
        df = df.iloc[:,0:6]
        df.columns = ['datetime','open','high','low','close','volume']

        df.open = df.open.astype("float")
        df.high = df.high.astype("float")
        df.low = df.low.astype("float")
        df.close = df.close.astype("float")
        df.volume = df.volume.astype("float")

        # No stock split and dividend announcement, hence close is same as adjusted close
        df['adj_close'] = df['close']

        df['datetime'] = [datetime.fromtimestamp(
            x / 1000.0) for x in df.datetime
        ]
        df.index = [x for x in range(len(df))]
        return df
    
    def dataframe_with_limit(self):
        df_list = []
        last_datetime = self.start_time
        while True:
            print(last_datetime)
            new_df = self.get_binance_bars(last_datetime)
            if new_df is None:
                break
            df_list.append(new_df)
            last_datetime = max(new_df.datetime) + timedelta(days=1)

        final_df = pd.concat(df_list)
        final_df['date'] = [x.strftime('%Y-%m-%d') for x in final_df['datetime']]
        return final_df

if __name__ == '__main__':
    hist_data = get_binance_data('BTCUSDT','1d','2021-01-01','2021-03-01')
    final_df = hist_data.dataframe_with_limit()
    final_df.to_csv('Bitcoin.csv')

        

