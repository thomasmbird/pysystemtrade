import datetime
from ib_insync import *
import pandas as pd
import csv
import os

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

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=2)

file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = 'top_of_book_'

file_name, file_mode = get_file_name(file_dir, file_name_base)

contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)

req_fields = ['time','bid','bidSize','ask','askSize','last','lastSize','prevBid','prevBidSize','prevAsk','prevAskSize','prevLast','prevLastSize','volume','open','high','low','close']

if file_mode == "Create":

    if not os.path.exists(file_name):
        print("Creating {}".format(file_name))
        with open(file_name, "a", newline="") as filetowrite:
            writer = csv.writer(filetowrite, delimiter=',')
            writer.writerow(req_fields)

stop_trigger = False

while not stop_trigger:
    with open(file_name, "a", newline="") as filetowrite:

        writer = csv.writer(filetowrite, delimiter=',')

        ticks = ib.reqMktData(contract,"",True,False)

        ib.sleep(0.1)
        print('TIME: ',ticks.time)
        print('BID PRICE/SIZE: ',ticks.bid,' ',ticks.bidSize,' BID ASK/SIZE: ',ticks.ask,' ',ticks.askSize, ' BID/ASK SPREAD: ', ticks.ask-ticks.bid)

        writer.writerow(
            [ticks.time,ticks.bid,ticks.bidSize,ticks.ask,ticks.askSize,ticks.last,ticks.lastSize,ticks.prevBid,ticks.prevBidSize,ticks.prevAsk,ticks.prevAskSize,ticks.prevLast,ticks.prevLastSize,ticks.volume,ticks.open,ticks.high,ticks.low,ticks.close])

    ib.sleep(5)


