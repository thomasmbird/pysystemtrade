import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE

base_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/CL/'

contract_list = ['CLF4','CLF5','CLG4','CLH4','CLJ4','CLK4','CLM4','CLN4','CLQ4','CLU4','CLV4','CLX4','CLZ3','CLZ4']

#contract_list = ['CLZ3']

file_list = []

for contr in contract_list:
    files = os.listdir(base_dir+contr)
    files.sort()

    for file in files:
        file_list.append(contr+'/'+file)

print(file_list)

fivemin_bar_header = ['index','endtime','open','high','low','close','volume','wap','count']

store = Arctic('localhost')
store.initialize_library('live_IBKR_data', lib_type=TICK_STORE)
lib = store['live_IBKR_data']
lib.list_symbols()

exit()
top_level_logging_dict = pd.DataFrame()

for filename in file_list:
    print(filename)
    contr = filename.split('/')[0]
    #print(filename.split('/')[0])

    loaded = pd.read_csv(base_dir+filename,header=0,names=fivemin_bar_header,parse_dates=['index'])
    if len(top_level_logging_dict) == 0:
        top_level_logging_dict = loaded
    else:
        top_level_logging_dict = pd.concat([top_level_logging_dict,loaded],ignore_index=True)

    loaded = loaded.drop(columns=['endtime'])
    asdict = loaded.to_dict(orient='records')

    print('Number of records: {}'.format(len(asdict)))

    ctr = 0
    for item in asdict:
        #print(item)
        #print(contr + '_5sec_bars_test')
        try:
            lib.write(contr+'_5sec_bars', [item])
        except Exception as e:
            print(e)
            print('Failed on: {}'.format(ctr))
            print(item)
        ctr+=1

print(top_level_logging_dict.tail(20).to_string())
print(top_level_logging_dict.shape)
print(ctr)