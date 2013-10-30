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


if __name__ == '__main__':
    count = 0
    orders = []
    symbols = []
    initial_cash = 100000.0
    reader  = csv.reader(open('ordershw6.csv', 'rU'), delimiter = ',')
    for row in reader:
        date = dt.datetime(int(row[0]), int(row[1]), int(row[2]),16, 0, 0)
        symbol = row[3]
        buy_or_sell = row[4]
        quantity = float(row[5])
        count = count+1
        orders.append([date, symbol, buy_or_sell, quantity])
        symbols.append(symbol)
    #print orders
    #print symbols

    #find unique symbols
    ls_symbols = list(set(symbols))

    #sort the order matrix
    orders.sort()

    #Start and end date
    dt_start = orders[0][0]
    dt_end = orders[-1][0]
    dt_timeofday = dt.timedelta(hours=16)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt_timeofday)

    #Creating an object of the dataaccess class with Yahoo as the source.
    c_dataobj = da.DataAccess('Yahoo')

    #Keys to be read from the data
    ls_keys = ['open', 'high', 'low', 'close', 'volume', 'actual_close']

    ldf_data = c_dataobj.get_data(ldt_timestamps, ls_symbols, ls_keys)
    d_data = dict(zip(ls_keys, ldf_data))

    #Copying close prices to a separate dataframe
    df_close = d_data['close'].copy()

    #filling the data
    df_close = df_close.fillna(method='ffill')
    df_close = df_close.fillna(method='bfill')

    #create the trade matrix
    trade = np.zeros((len(ldt_timestamps), len(ls_symbols)))
    trade = pd.DataFrame(trade, index = ldt_timestamps, columns = ls_symbols)
    cash = pd.TimeSeries(0.0, ldt_timestamps)
    cash.ix[ldt_timestamps[0]] = float(100000.0)
    holdings  = pd.TimeSeries(0.0, ldt_timestamps)
    temp_cash = pd.TimeSeries(0.0, ldt_timestamps)
    #iterate over orders list and enter the value of stock traded on any particular day into the trade matrix
    temp_holdings = 0.0
    for i in range(0, len(ldt_timestamps)):
        for j in range(0, count):
            if ldt_timestamps[i] == orders[j][0]:
                idx = ls_symbols.index(orders[j][1])
                if orders[j][2] == 'Buy':
                    # Calculate the no. of shares traded on that day and store it in the trade matrix
                    trade.ix[ldt_timestamps[i]][orders[j][1]] += orders[j][3]
                    #temp_cash[ldt_timestamps[i]] -= df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #if i!=0:
                    #    cash.ix[ldt_timestamps[i]] = cash.ix[ldt_timestamps[i-1]] - df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #else:
                    #    cash.ix[ldt_timestamps[i]] -= df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #print cash.ix[ldt_timestamps[i]]
                    # Calculate the amount used to buy the shares and update the cash matrix
                    #cash.ix[orders[j][0]] -= df_close.iloc[i, idx]*orders[j][3]
                    #temp_cash = cash.ix[orders[j][0]]

                else:
                    trade.ix[ldt_timestamps[i]][orders[j][1]] -= orders[j][3]
                    #temp_cash[ldt_timestamps[i]] += df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #if i!=0:
                    #    cash.ix[ldt_timestamps[i]] = cash.ix[ldt_timestamps[i-1]] + df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #else:
                    #    cash.ix[ldt_timestamps[i]] += df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #print cash.ix[ldt_timestamps[i]]
                    #cash.ix[orders[j][0]] += df_close.iloc[i, idx]*orders[j][3]
                    #temp_cash = cash.ix[orders[j][0]]
                    #print cash.ix[orders[j][0]]
                    #print trade.ix[ldt_timestamps[i]][orders[j][1]]
                    #temp_cash += trade.ix[ldt_timestamps[i]][orders[j][1]]*df_close.iloc[i, idx]
                    #print temp_cash
            #else:
             #cash.ix[ldt_timestamps[i]] = cash.ix[ldt_timestamps[i-1]]


    for i in range(0, len(ldt_timestamps)):
        for j in range(0, count):
            if ldt_timestamps[i] == orders[j][0]:
                idx = ls_symbols.index(orders[j][1])
                temp_cash.ix[ldt_timestamps[i]] += df_close.iloc[i, idx]*trade.ix[ldt_timestamps[i]][orders[j][1]]

         
    cash.ix[ldt_timestamps[0]] -= temp_cash.ix[ldt_timestamps[0]]
    for i in range(1, len(ldt_timestamps)):
        cash.ix[ldt_timestamps[i]] = cash.ix[ldt_timestamps[i-1]] - temp_cash.ix[ldt_timestamps[i]]

    portfolio = trade
    holdings[ldt_timestamps[0]] = initial_cash;
    for i in range(1, len(ldt_timestamps)):
        portfolio.ix[ldt_timestamps[i]] += portfolio.ix[ldt_timestamps[i-1]]
        holdings[ldt_timestamps[i]] = cash[ldt_timestamps[i]] + np.dot(portfolio.ix[ldt_timestamps[i]].values.astype(float), df_close.iloc[i])
    
    writer = csv.writer(open('results_hw6.csv', 'wb'), delimiter = ',')
    for row_index in holdings.index:
        row_to_enter = [str(row_index.year), str(row_index.month), str(row_index.day), str(holdings[row_index])]
        #print row_to_enter
        writer.writerow(row_to_enter)
    
    #------------------------------------------------------------------------
    # Analyze
    #------------------------------------------------------------------------

    hol = holdings
    #print hol.values
    na_rets  = holdings.values
    na_rets = na_rets / na_rets[0]
    #print  na_rets
    tsu.returnize0(na_rets)

    stddev = np.std(na_rets)
    daily_ret = np.mean(na_rets)
    sharpe =  (np.sqrt(252) * daily_ret) / stddev
    print "stddev  ", stddev
    print "sharpe   ", sharpe

    
    
    
