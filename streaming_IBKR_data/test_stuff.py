import datetime
from ib_insync import *
import pandas as pd

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=1)

#contract = Stock('TSLA', 'SMART', 'USD')

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
contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)

ticker = ib.reqMktDepth(contract)

df = pd.DataFrame(index=range(5),
        columns='timestamp bidSize bidPrice askPrice askSize'.split())

def onTickerUpdate(ticker):
    bids = ticker.domBids
    for i in range(5):
        df.iloc[i, 1] = bids[i].size if i < len(bids) else 0
        df.iloc[i, 2] = bids[i].price if i < len(bids) else 0
    asks = ticker.domAsks
    for i in range(5):
        df.iloc[i, 3] = asks[i].price if i < len(asks) else 0
        df.iloc[i, 4] = asks[i].size if i < len(asks) else 0
    df.iloc[:, 0] = ticker.time
    #clear_output(wait=True)
    #display(df)
    print(df.to_string())

input_string = ""
while input_string != "stop":
    ticker.updateEvent += onTickerUpdate

    ib.sleep(15)
    #input_string = input("Type stop to stop")


ib.cancelMktDepth(contract)
ib.disconnect()

#def onBarUpdate(bars, hasNewBar):
#    print(bars[-1])

#bars = ib.reqRealTimeBars(contract, 5, 'MIDPOINT', False)
#bars.updateEvent += onBarUpdate

#ib.sleep(30)
#print(bars)

#ib.cancelRealTimeBars(bars)



#ib.reqRealTimeBars('100',Contract('3001'), 495512552, 5, "MIDPOINT", True)

#hist = ib.reqHistoricalData(contract,barSizeSetting="5 mins",endDateTime="",
#    durationStr="1 D",
##    whatToShow='MIDPOINT',
#    useRTH=True
#    )

#running = True
#while running:
    # This updates IB-insync:

    #ib.reqMktData(contract,"", True, False)
    #ib.sleep(10)
#ib.sleep(20)
#ib.cancelRealTimeBars('100')