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
    dt_start = dt.datetime(2008, 1, 1)
    dt_end = dt.datetime(2009, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))
    dataobj = da.DataAccess('Yahoo')
    ls_symbols = dataobj.get_symbols_from_list('sp5002012')
    ls_symbols.append('SPY')
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']
    ldf_data = dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    df_close = d_data['close'].copy()
    df_close = df_close.fillna(method='ffill')
    df_close = df_close.fillna(method='bfill')
    ts_market = df_close['SPY']

    writer = csv.writer(open('ordershw6.csv', 'wb'), delimiter=',')
    
    df_mean = pd.rolling_mean(df_close, lookback)
    std_dev = pd.rolling_std(df_close, lookback)
    bollinger = (df_close - df_mean) / std_dev
    print bollinger.values
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN
    count=0
    for s_sym in ls_symbols:
        for i in range(1, len(ldt_timestamps)):
            btoday = bollinger[s_sym].ix[ldt_timestamps[i]]
            byest = bollinger[s_sym].ix[ldt_timestamps[i-1]]
            bspy = bollinger['SPY'].ix[ldt_timestamps[i]]

            if ((btoday<-2.0) and (byest>=-2.0) and (bspy>=1.4)):
                df_events[s_sym].ix[ldt_timestamps[i]] = 1
                count = count+1;
                row_to_enter = [str(ldt_timestamps[i].year), str(ldt_timestamps[i].month), str(ldt_timestamps[i].day), s_sym, 'Buy', 100]
                writer.writerow(row_to_enter)
                try:
                    time_n = ldt_timestamps[i + 5]
                except:
                    time_n = ldt_timestamps[-1]
                row_to_enter = [str(time_n.year), str(time_n.month), \
                        str(time_n.day), s_sym, 'Sell', 100]
                writer.writerow(row_to_enter)                
                                
    df_events.to_csv("HW6Events.csv",sep=",", index=True, Header= True) 
    print count
    ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
                 s_filename='hw6EventStudy_quiz.pdf', b_market_neutral=True, b_errorbars=True,
                s_market_sym='SPY')
    
