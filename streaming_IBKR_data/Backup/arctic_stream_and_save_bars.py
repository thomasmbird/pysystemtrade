import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE
import time as tyme

def get_file_name(file_dir,file_name_base):
    today = datetime.datetime.today().strftime('%m_%d_%y')
    tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

    if datetime.datetime.today().hour > 16:
        log_date = tomorrow
    else:
        log_date = today
    print('Choosing log date: ',log_date)
    file = file_dir+file_name_base+log_date+'.csv'
    if os.path.exists(file):
        print('{} already exists.'.format(file))
        file_mode = "append"
    else:
        print('{} will be created.'.format(file))
        file_mode="create"
    return file, file_mode
def onBarUpdate(bars, hasNewBar):
    bardict = bars[-1].dict()
    print(bardict)
    bardict['index'] = bardict['time']
    #bardict['time'] = bardict['time'] + datetime.timedelta(hours=6)
    lib.write('ESZ3_5sec', [{'index': bardict['time'],
                             'open' : bardict['open_']   ,
                             'high' : bardict['high']   ,
                             'low' : bardict['low']      ,
                             'close' : bardict['close']      ,
                             'volume' :bardict['volume']      ,
                             'count' : bardict['count']
                             }])
    with open(file_name, "a", newline="") as filetowrite:
        writer_internal = csv.writer(filetowrite, delimiter=',')
        print('ESZ3: ',bars[-1].dict())
        bardict = bars[-1].dict()
        writer_internal.writerow(
            [bardict['time'], bardict['endTime'], bardict['open_'], bardict['high'], bardict['low'], bardict['close'],
             bardict['volume'], bardict['wap'], bardict['count']])

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=48)

store = Arctic('localhost')
#store.initialize_library('live_5sec_bars', lib_type=TICK_STORE)
lib = store['live_IBKR_data']

req_fields = ['timestamp','endtime','open','high','low','close','volume','wap','count']
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = '5sec_bars_'

file_name, file_mode = get_file_name(file_dir, file_name_base)
if file_mode == "create":
    with open(file_name, "a", newline="") as filetowrite:
        writer = csv.writer(filetowrite, delimiter=',')
        writer.writerow(req_fields)



contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)

print(contract)

print(lib.min_date('ESZ3_5sec_bars'))
print(lib.max_date('ESZ3_5sec_bars'))
last_date = lib.max_date('ESZ3_5sec_bars')
print("LAST DATE IN Df: ",last_date)

now = datetime.datetime.now()-datetime.timedelta(hours=6)-datetime.timedelta(minutes=2)
endtr = now+datetime.timedelta(days=3)
tes_rng = pd.date_range(start=now.strftime("%Y-%m-%d %H:%M:%S"), end=endtr.strftime("%Y-%m-%d %H:%M:%S"), freq="D")
print(tes_rng)

start_date = last_date - datetime.timedelta(hours=11)  #- datetime.timedelta(minutes=30)
end_date = last_date + datetime.timedelta(days=1)
dr = pd.date_range(start_date,end_date,freq='D')
print(dr)
#start=now.strftime("%Y-%m-%d %H:%M:%S"), end=endtr.strftime("%Y-%m-%d %H:%M:%S"), freq="D")
print(last_date)
print(type(last_date))
#print(lib.min_date('ESZ3_5sec_bars'))

tt = tyme.time()
# new_market_df = lib.read('ESZ3_5sec_bars', columns=None,date_range=dr)
# print("Took {} to load full data".format(tyme.time() - tt))
# new_market_df.index = new_market_df.index.shift(1, freq='H')
# new_market_df.sort_index()
# print(new_market_df.shape)
# print(new_market_df.head(5).to_string())
# print(new_market_df.tail(5).to_string())

#req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
all_bars = []
all_bars_df = pd.DataFrame()

req_time_list = ["20231119 18:00:00 US/Central",
            "20231119 19:00:00 US/Central",
            "20231119 20:00:00 US/Central",
            "20231119 21:00:00 US/Central",
            "20231119 22:00:00 US/Central",
            "20231119 23:00:00 US/Central",
            "20231120 00:00:00 US/Central",
            "20231120 01:00:00 US/Central",
            "20231120 02:00:00 US/Central",
            "20231120 03:00:00 US/Central",
            "20231120 04:00:00 US/Central",
            "20231120 05:00:00 US/Central",
            "20231120 06:00:00 US/Central",
            "20231120 07:00:00 US/Central"]

for req_time in req_time_list:
    print(" Starting : ",req_time)
    bars = ib.reqHistoricalData(contract, endDateTime=req_time,
        durationStr='3600 S',
        barSizeSetting='5 secs',
        whatToShow='TRADES',
        useRTH=False,
        formatDate=1)
    if len(bars) != 0:
        for bar in bars:
            all_bars.append(bar)
        #if all_bars_df.shape[0] == 0:
        #    barsnewdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])


        for iy in range(0, len(bars)):
            barsnewdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])  # endtr = now+datetime.timedelta(days=3)
# print(bars[iy].dict())# rng = pd.date_range(start=now.strftime("%Y-%m-%d %H:%M:%S"), end=endtr.strftime("%Y-%m-%d %H:%M:%S"), freq="D")
            barsnewdf.loc[iy] = [bars[iy].date.strftime("%m/%d/%Y %H:%M:%S"), bars[iy].open, bars[iy].high, bars[iy].low,# new_market_df = lib.read('ESZ3_5sec_bars', columns=None, date_range=rng)
                      bars[iy].close,# new_market_df.index = new_market_df.index.shift(1, freq='H')
                      bars[iy].volume, bars[iy].average, bars[iy].barCount]#
        if all_bars_df.shape[0] == 0:
            all_bars_df = barsnewdf
        else:
            all_bars_df = pd.concat([all_bars_df, barsnewdf], ignore_index=True)
        print(all_bars_df.tail(5).to_string())


print("All done, adding to DB")
print(len(all_bars))
print(all_bars[0],all_bars[-1])
list_to_write = []
for item in all_bars:
    list_to_write.append(
        {'index:': item.time,
         'open' :  item.open,
         'high' :  item.high,
         'low' :  item.low,
        'close' :  item.close,
         'volume' :  item.volume,
         'count': item.count})
print(len(list_to_write))
all_bars_df.to_csv('/Users/thomasmbird/Documents/Quant Stuff/new_es_bars.csv')
#bardict['count' : bardict['count']    ,   'time'] = bardict['time'] + datetime.timedelta(hours=6)
lib.write('ESZ3_5sec', list_to_write)
print("Done")
print("New last date: ",lib.max_date('ESZ3_5sec_bars'))





#
#
# stop_flag = False
# while not stop_flag:
#     ib.sleep(60*60*60)
#
# ib.cancelRealTimeBars(bars)
#