import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE

class IBFutures(Contract):
    def __init__(self, conId:str, symbol:str,  exchange:str, secType:str,
                 lastTradeDateOrContractMonth: str,
                 currency = 'USD', localSymbol = ""):
        Contract.__init__(self)

        self.conId = conId
        self.symbol = symbol
        self.secType = secType
        self.exchange = exchange
        self.currency = currency
        self.localSymbol = localSymbol
        self.lastTradeDateOrContractMonth = lastTradeDateOrContractMonth

    def qual_contr(self):
        ib.qualifyContracts(self)
        print('Successfully qualified ',self.localSymbol)

    def onBarUpdate(self, bars, hasNewBar):
        with open(file_dir + self.localSymbol + '/' + file_name, "a", newline="") as filetowrite:
            writer_internal = csv.writer(filetowrite, delimiter=',')
            print(self.localSymbol,bars[-1].tuple())
            bardict = bars[-1].dict()
            writer_internal.writerow(
                [bardict['time'], bardict['endTime'], bardict['open_'], bardict['high'], bardict['low'], bardict['close'],
                 bardict['volume'], bardict['wap'], bardict['count']])

        try:
            lib.write(self.localSymbol+'_5sec_bars', [{'index': bardict['time'],
                                     'open': bardict['open_'],
                                     'high': bardict['high'],
                                     'low': bardict['low'],
                                     'close': bardict['close'],
                                     'volume': bardict['volume'],
                                     'count': bardict['count']
                                     }])
        except Exception as e:
            print(e)

    def stream_bars(self):
        print('Starting to stream ', self.localSymbol)
        if not os.path.exists(file_dir+self.localSymbol+'/'):
            os.makedirs(file_dir+self.localSymbol+'/')
            print('Made directory for contract: ', self.localSymbol)

        req_fields = ['timestamp', 'endtime', 'open', 'high', 'low', 'close', 'volume', 'wap', 'count']

        if not os.path.exists(file_dir+self.localSymbol+'/'+file_name):
            print("Creating file: "+file_dir+self.localSymbol+'/'+file_name)
            with open(file_dir+self.localSymbol+'/'+file_name, "w", newline="") as filetowrite:
                writer = csv.writer(filetowrite, delimiter=',')
                writer.writerow(req_fields)
        else:
            print("File already existed, appending")

        bars = ib.reqRealTimeBars(self, 5, 'TRADES', False)
        bars.updateEvent += self.onBarUpdate

        print('Successfully streaming ', self.localSymbol)

def get_file_name(file_dir,file_name_base):
    today = datetime.datetime.today().strftime('%m_%d_%y')
    tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')
    if datetime.datetime.today().hour > 16:
        log_date = tomorrow
    else:
        log_date = today
    print('Choosing log date: ',log_date)
    file_name = file_name_base+log_date+'.csv'
    return file_name

store = Arctic('localhost')
store.initialize_library('live_IBKR_data', lib_type=TICK_STORE)
lib = store['live_IBKR_data']

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=99)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/5SecBars/'
file_name_base = '5sec_bars_'

file_name= get_file_name(file_dir, file_name_base)

list_of_contracts = pd.read_csv('/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/allbarstemp.csv')
#rates_to_add = pd.read_csv('/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/treasury_contracts.csv')

contract_object_list = []
for ix in range(0,list_of_contracts.shape[0]):
    contract_object_list.append(IBFutures(list_of_contracts['conId'].iloc[ix],list_of_contracts['symbol'].iloc[ix],list_of_contracts['exchange'].iloc[ix],list_of_contracts['secType'].iloc[ix],
                                          list_of_contracts['lastTradeDateOrContractMonth'].iloc[ix],list_of_contracts['currency'].iloc[ix],))

for obj in contract_object_list:
    print(obj.conId)
    print(obj.qual_contr())
    obj.stream_bars()

stop_flag = False
while not stop_flag:
    ib.sleep(60*60*60)

req_fields = ['conId','timestamp','endtime','open','high','low','close','volume','wap','count']


ib.disconnect()
exit()

