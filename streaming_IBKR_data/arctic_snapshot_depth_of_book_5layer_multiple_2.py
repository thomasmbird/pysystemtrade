import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE
import time as tyme

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
    def snap_top_of_book(self):
        #print('Starting to stream ', self.localSymbol)
        if not os.path.exists(file_dir+self.localSymbol+'/'):
            os.makedirs(file_dir+self.localSymbol+'/')
            print('Made directory for contract: ', self.localSymbol)

        #req_fields = ['time','bid','bidSize','ask','askSize','last','lastSize','prevBid','prevBidSize','prevAsk','prevAskSize','prevLast','prevLastSize','volume','open','high','low','close']
        req_fields = ['time','level','bidSize','bidPrice','askPrice','askSize']

        if not os.path.exists(file_dir+self.localSymbol+'/'+file_name):
            print("Creating file: "+file_dir+self.localSymbol+'/'+file_name)
            with open(file_dir+self.localSymbol+'/'+file_name, "w", newline="") as filetowrite:
                writer = csv.writer(filetowrite, delimiter=',')
                writer.writerow(req_fields)
        #else:
        #    print("File already existed, appending")

        with open(file_dir+self.localSymbol+'/'+file_name, "a", newline="") as filetowrite:
            writer = csv.writer(filetowrite, delimiter=',')

            #ticks = ib.reqMktData(self, "", True, False)
            ticks = ib.reqMktDepth(self, numRows=10)

            ib.sleep(0.5)
            #print(len(domTicks))
            #print('TIME: ', ticks.time)
            #print(type(ticks.time))
            #print(self.localSymbol,' BID PRICE/SIZE: ', ticks.bid, ' ', ticks.bidSize, ' BID ASK/SIZE: ', ticks.ask, ' ', ticks.askSize,
            #      ' BID/ASK SPREAD: ', ticks.ask - ticks.bid)
            if len(ticks.domTicks) == 0:
                print("Waiting on ",self.localSymbol)
                ib.sleep(0.5)

            if len(ticks.domTicks) != 0:
                print(ticks)
                print(ticks.domTicks[0])
                print(len(ticks.domTicks))
                print(self.localSymbol,0,ticks.domTicks[0].time,ticks.domBids[0].size,ticks.domBids[0].price,ticks.domAsks[0].price,ticks.domAsks[0].size)

                for i in range(0, 10):

                    try:
                        lib.write(self.localSymbol+'_full_book', [{'index': ticks.domTicks[0].time,
                                                                   'level': i,
                                                        'bid': ticks.domBids[i].price,
                                                        'bidSize': ticks.domBids[i].size,
                                                        'ask': ticks.domAsks[i].price,
                                                        'askSize': ticks.domAsks[i].size}])

                    except Exception as e:
                        print(self.localSymbol, i, e)
                    try:
                        writer.writerow(
                            [ticks.domTicks[0].time, i, ticks.domBids[i].size,ticks.domBids[i].price,ticks.domAsks[i].price,ticks.domAsks[i].size])
                    except Exception as e:
                        print(self.localSymbol, i, e)

        ib.cancelMktDepth(self)

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
ib.connect('127.0.0.1', 4001, clientId=48)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/FullBook/'
file_name_base = 'full_book_'

file_name = get_file_name(file_dir, file_name_base)

list_of_contracts = pd.read_csv('/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/allbars_2.csv')

contract_object_list = []
for ix in range(0,list_of_contracts.shape[0]):
    contract_object_list.append(IBFutures(list_of_contracts['conId'].iloc[ix],list_of_contracts['symbol'].iloc[ix],list_of_contracts['exchange'].iloc[ix],list_of_contracts['secType'].iloc[ix],
                                          list_of_contracts['lastTradeDateOrContractMonth'].iloc[ix],list_of_contracts['currency'].iloc[ix],))

for obj in contract_object_list:
    print(obj.conId)
    print(obj.qual_contr())

print("Got through all contracts")
#print(list_of_contracts.to_string())
print(list_of_contracts.shape)

stop_trigger = False
curr_hour = 18
while curr_hour != 16:
    curr_hour = datetime.datetime.now().hour
    print("Current hour: {}", curr_hour)
    st = tyme.time()
    for obj in contract_object_list:
        obj.snap_top_of_book()
    print("Time elapsed to snap all books: {}".format(tyme.time()-st))
    if datetime.datetime.now().hour < 7 or datetime.datetime.now().hour > 16:
        ib.sleep(90)
    else:
        ib.sleep(8)

stop_flag = False
while not stop_flag:
    ib.sleep(60*60*60)

req_fields = ['conId','timestamp','endtime','open','high','low','close','volume','wap','count']


ib.disconnect()
exit()
