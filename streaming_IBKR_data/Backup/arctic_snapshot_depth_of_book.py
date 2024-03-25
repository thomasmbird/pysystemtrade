import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE

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

store = Arctic('localhost')
store.initialize_library('live_IBKR_data', lib_type=TICK_STORE)
lib = store['live_IBKR_data']

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=20)

file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = 'top_of_book_'

file_name, file_mode = get_file_name(file_dir, file_name_base)
#file_mode = 'Create'

contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)

req_fields = ['time','bid','bidSize','ask','askSize','last','lastSize','prevBid','prevBidSize','prevAsk','prevAskSize','prevLast','prevLastSize','volume','open','high','low','close']

if file_mode == "Create":
    if not os.path.exists(file_name):
        print("Creating {}".format(file_name))
        with open(file_name, "w", newline="") as filetowrite2:
            writer2 = csv.writer(filetowrite2, delimiter=',')
            writer2.writerow(req_fields)

stop_trigger = False

while not stop_trigger:
    with open(file_name, "a", newline="") as filetowrite:

        writer = csv.writer(filetowrite, delimiter=',')

        ticks = ib.reqMktData(contract,"",True,False)

        ib.sleep(0.1)
        print('TIME: ',ticks.time)
        print(type(ticks.time))
        print('BID PRICE/SIZE: ',ticks.bid,' ',ticks.bidSize,' BID ASK/SIZE: ',ticks.ask,' ',ticks.askSize, ' BID/ASK SPREAD: ', ticks.ask-ticks.bid)

        try:
             lib.write('ESZ3_top_of_book', [{'index': ticks.time,
                                             'bid': ticks.bid,
                                             'bidSize': ticks.bidSize,
                                             'ask': ticks.ask,
                                             'askSize': ticks.askSize,
                                             'last': ticks.last,
                                             'lastSize': ticks.lastSize,
                                             'prevBid': ticks.prevBid,
                                             'prevBidSize': ticks.prevBidSize,
                                             'prevAsk': ticks.prevAsk,
                                             'prevAskSize': ticks.prevAskSize,
                                             'prevLast': ticks.prevLast ,
                                             'prevLastSize': ticks.prevLastSize,
                                             'volume': ticks.volume ,
                                             'open': ticks.open,
                                             'high': ticks.high,
                                             'low': ticks.low,
                                             'close': ticks.close}])
        except Exception as e:
            print(e)
        writer.writerow(
            [ticks.time,ticks.bid,ticks.bidSize,ticks.ask,ticks.askSize,ticks.last,ticks.lastSize,ticks.prevBid,ticks.prevBidSize,ticks.prevAsk,ticks.prevAskSize,ticks.prevLast,ticks.prevLastSize,ticks.volume,ticks.open,ticks.high,ticks.low,ticks.close])

    ib.sleep(5)


