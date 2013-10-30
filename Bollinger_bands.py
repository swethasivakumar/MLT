import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import csv
import QSTK.qstkstudy.EventProfiler as ep
import csv
import matplotlib.pyplot as plt

if __name__ == '__main__':
    lookback = 20
    index = 0
    dt_start = dt.datetime(2010, 1, 1)
    dt_end = dt.datetime(2010, 12, 31)
    d = dt.datetime(2010, 6, 14, 16, 0, 0)
    print d
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    for i in range(1, len(ldt_timestamps)):
        #print ldt_timestamps[i]
        if ldt_timestamps[i] == d:
            index = i;
            print i
    print index;
    dataobj = da.DataAccess('Yahoo')
    ls_symbols = ['MSFT']
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))
    df_close = d_data['close'].copy()
    df_close = df_close.fillna(method='ffill')
    df_close = df_close.fillna(method='bfill')
    
    df_mean = pd.rolling_mean(df_close, lookback)
    std_dev = pd.rolling_std(df_close, lookback)

    bollinger = pd.TimeSeries(0.0, ldt_timestamps)
    #print df_mean.values
    #print std_dev.values
    
    bollinger = (df_close - df_mean)/std_dev
    #plt.plot(ldt_timestamps, bollinger, label = 'None')
    print bollinger.values[index]

    writer = csv.writer(open('bollinger_results.csv', 'wb'), delimiter = ',')
    for row_index in bollinger.index:
        row_to_enter = [str(row_index.year), str(row_index.month), str(row_index.day), str(bollinger[row_index])]
        writer.writerow(row_to_enter)        
        

    
    
