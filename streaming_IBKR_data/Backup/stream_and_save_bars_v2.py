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

def onBarUpdate(bars, hasNewBar):

    with open(file_name, "a", newline="") as filetowrite:
        writer_internal = csv.writer(filetowrite, delimiter=',')
        print('ESZ3: ',bars[-1].dict())
        bardict = bars[-1].dict()
        writer_internal.writerow(
            [bardict['time'], bardict['endTime'], bardict['open_'], bardict['high'], bardict['low'], bardict['close'],
             bardict['volume'], bardict['wap'], bardict['count']])


ib = IB()
ib.connect('127.0.0.1', 4001, clientId=1)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'

file_name_base = '5sec_bars_'

file_name, file_mode = get_file_name(file_dir, file_name_base)

contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)


req_fields = ['timestamp','endtime','open','high','low','close','volume','wap','count']

if file_mode == "create":
    with open(file_name, "a", newline="") as filetowrite:
        writer = csv.writer(filetowrite, delimiter=',')
        writer.writerow(req_fields)

bars = ib.reqRealTimeBars(contract, 5, 'TRADES', False)
bars.updateEvent += onBarUpdate

stop_flag = False
while not stop_flag:
    ib.sleep(60*60*60)

ib.cancelRealTimeBars(bars)
