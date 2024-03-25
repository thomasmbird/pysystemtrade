import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE

def onBarUpdate(bars, hasNewBar):
    bardict = bars[-1].dict()
    print(bardict)
    bardict['index'] = bardict['time']
    lib.write('ESZ3_5sec', [{'index': bardict['time'],
                             'open' : bardict['open_']   ,
                             'high' : bardict['high']   ,
                             'low' : bardict['low']      ,
                             'close' : bardict['close']      ,
                             'volume' :bardict['volume']      ,
                             'count' : bardict['count']
                             }])


base_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
#mkt = 'ES'
file_dict={}
file_dict['ES/tickers_10_24_23.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_24_23_1.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_24_23_2.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_25_23.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_25_23_1.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_26_23.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_26_23_1.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_26_23_3.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_26_23_4.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_27_23.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_27_23_2.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_27_23_3.csv'] = "live_ES_top_of_book"
file_dict['ES/tickers_10_30_23.csv'] = "live_ES_top_of_book"
file_dict['ES/top_of_book_10_30_23.csv'] = "live_ES_top_of_book"
file_dict['ES/top_of_book_10_31_23.csv'] = "live_ES_top_of_book"
file_dict['ES/top_of_book_11_01_23.csv'] = "live_ES_top_of_book"

header_dict = {}
header_dict['live_ES_top_of_book'] = ['index', 'bid', 'bidSize', 'ask', 'askSize', 'last', 'lastSize', 'prevBid', 'prevBidSize', 'prevAsk', 'prevAskSize', 'prevLast', 'prevLastSize', 'volume', 'open', 'high', 'low', 'close']

type = 'live_ES_top_of_book'

lib_symbol = 'ESZ3_top_of_book'

store = Arctic('localhost')
store.initialize_library('live_IBKR_data', lib_type=TICK_STORE)
lib = store['live_IBKR_data']
top_level_logging_dict = pd.DataFrame()

for filename in file_dict.keys():
    print(filename)

    loaded = pd.read_csv(base_dir+filename,header=0,names=header_dict[file_dict[filename]],parse_dates=['index'])
    if len(top_level_logging_dict) == 0:
        top_level_logging_dict = loaded
    else:
        top_level_logging_dict = pd.concat([top_level_logging_dict,loaded],ignore_index=True)

    asdict = loaded.to_dict(orient='records')
    print(len(asdict))
    print('Number of records: {}'.format(len(asdict)))

    ctr = 0
    for item in asdict:
        #try:
        #    lib.write(lib_symbol, [item])
        #except Exception as e:
        #    print(e)
        #    print('Failed on: {}'.format(ctr))
        #    print(item)
        ctr+=1

print(top_level_logging_dict.tail(20).to_string())
print(top_level_logging_dict.shape)
print(ctr)