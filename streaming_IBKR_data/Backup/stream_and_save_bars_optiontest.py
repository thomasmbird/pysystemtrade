import datetime
from ib_insync import *
import pandas as pd
import csv

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=40)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/Options/'
file = '5sec_bars_10_27_23_2.csv'
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))

#sub = ScannerSubscription(
#    instrument='ESZ3',
#    locationCode='CME',
#    code =
#)

#sub = ScannerSubscription(495512552)
#contract = Future('ES',"ESZ3",'CME', currency='USD')
#contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
#contract = Contract(secType='FOP')
contract = Contract(symbol='ES', secType='FOP', lastTradeDateOrContractMonth='20231031', strike=4200, right='P', exchange='CME', currency='USD')#, tradingClass='EW')
print(contract)
ib.qualifyContracts(contract)
print('done')
exit()
#ticker = ib.reqMktDepth(contract)


####

def onBarUpdate(bars, hasNewBar):

    with open(file_dir + file, "a", newline="") as filetowrite:
        writer_internal = csv.writer(filetowrite, delimiter=',')
        print('ESZ3: ',bars[-1].dict())
        bardict = bars[-1].dict()
        writer_internal.writerow(
            [bardict['time'], bardict['endTime'], bardict['open_'], bardict['high'], bardict['low'], bardict['close'],
             bardict['volume'], bardict['wap'], bardict['count']])


req_fields = ['timestamp','endtime','open','high','low','close','volume','wap','count']

#with open("output.csv", "a", newline="") as f_output:
#    csv_output = csv.DictWriter(f_output, fieldnames=req_fields, extrasaction="ignore")
##    csv_output.writeheader()        # only needed once
 #   csv_output.writerow(data['data'][0]['content'][0])
##
with open(file_dir+file, "a", newline="") as filetowrite:
    writer = csv.writer(filetowrite, delimiter=',')
    writer.writerow(req_fields)

bars = ib.reqRealTimeBars(contract, 5, 'TRADES', False)
bars.updateEvent += onBarUpdate

stop_flag = False
while not stop_flag:
    ib.sleep(60*60*60)

ib.cancelRealTimeBars(bars)
