import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
import numpy as np
pd.options.mode.chained_assignment = None
ib = IB()
ib.connect('127.0.0.1', 4001, clientId=22)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/Options/'
file_name_base = '0dte_'

today = datetime.datetime.today().strftime('%m_%d_%y')
tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

if datetime.datetime.today().hour > 16:
    log_date = tomorrow
else:
    log_date = today
print('Choosing log date: ',log_date)
file = file_dir+file_name_base+log_date


und_contract = Contract(secType='FUT',symbol='ES',currency='USD',localSymbol='ESZ3',exchange='CME')
ib.qualifyContracts(und_contract)
chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId)
#print('Time elapsed: ',tyme.time()-st)
#print(util.df(chains).to_string())
#exit()
#util.df(chains).to_csv(temp_dir+'chain_info.csv')
#exit()
#ib.reqMarketDataType(4)
expiry='20231103'

file = file+'_'+expiry+'_'

for c in chains:
    if expiry in c.expirations:
        strikes = c.strikes
        tradingClass = c.tradingClass
        undconid = c.underlyingConId
        exch = c.exchange
        mult = c.multiplier

strikes = [strike for strike in strikes if strike > 4100 and strike < 4400]

rights = ['C', 'P']
#strikes = [4180.0]

contracts = [Contract(symbol='ES', secType='FOP', multiplier = 50, lastTradeDateOrContractMonth=expiry, strike=strike, right=right, exchange='CME', currency='USD', tradingClass=tradingClass)
            for strike in strikes
            for right in rights]

print(len(contracts))
print(contracts)

for contract in contracts:
    try:
        print(ib.qualifyContracts(contract))
    except Exception as e:
        print("Failed to qualify contract: ", e)
        exit()
    #print("Finished ",contract)

stop_flag = False
df = pd.DataFrame()
tickerlist = []


while not stop_flag:
    try:
        st = tyme.time()
        outputlist = []

        for x in contracts:
            #"100,101,105,106,221,233"

            snapshot = ib.reqMktData(x,"", True, False)
            #ib.sleep(0.1)
            outputlist.append([x.strike, x.right, snapshot])
        print('Time elapsed calling reqMktData: ', tyme.time() - st)
        ib.sleep(2)
        ctr = 0
        st=tyme.time()
        #while util.isNan(outputlist[0][2].bid) or util.isNan(outputlist[-1][2].bid):
        while any([util.isNan(item[2].bid) for item in outputlist]):
            print('WAITING')
            #for item in outputlist:
            #    print(item[2])
            #    print(item[2].time)
            #    print(item[2].bid)
           # for item in outputlist:
           #     print(item[2].bidGreeks,item[2].askGreeks,item[2].modelGreeks)
                #print(item[2].bid)
                #if item[2].bid == "nan":
                #    print(item[2])
            #print(outputlist[]
            print(ctr)
            ib.sleep(1)
            ctr += 1
            #print([item[2].bid for item in outputlist])
        print('Time elapsed waiting for reqMktData: ', tyme.time() - st)
        print([item[2].bid for item in outputlist])
        st = tyme.time()
        #ib.sleep(20)
        #print('Time elapsed waiting for mkt data to fill in', tyme.time() - st)
        #st=tyme.time()

        #print(snapshot.dict())

        outdf = pd.DataFrame()

        counter = 0
        for output in outputlist:
            tempdict = {}
            datadict = output[2].dict()

            ##print(datadict['bidGreeks'])
            #print(datadict['askGreeks'])
            ##print(datadict['lastGreeks'])
            #print(datadict['modelGreeks'])
            #print(datadict.keys())
            #print(datadict.values())
            tempdict['strike'] = output[0]
            tempdict['putcall'] = output[1]
            tempdict['day'] = datadict['time'].strftime('%m/%d/%Y')
            tempdict['time'] = datadict['time'].strftime('%H_%M_%S')
            tempdict['bid'] = datadict['bid']
            tempdict['bidSize'] = datadict['bidSize']
            tempdict['ask'] = datadict['ask']
            tempdict['askSize'] = datadict['askSize']
            tempdict['last'] = datadict['last']
            tempdict['lastSize'] = datadict['lastSize']
            tempdict['open'] = datadict['open']
            tempdict['high'] = datadict['high']
            tempdict['low'] = datadict['low']
            tempdict['close'] = datadict['close']
            #tempdict['undPrice'] = datadict['bidGreeks'].undPrice
            #tempdict['undPrice2'] = datadict['askGreeks'].undPrice
            #print('here')
            listy = ['bid','ask','last','model']
            for ll in listy:
                #if datadict[ll+'Greeks']
                if datadict[ll+'Greeks'] == None:
                    tempdict[ll + '_undPrice'] = 0
                    tempdict[ll + '_tickAttrib'] = 0
                    tempdict[ll + '_impliedVol'] = 0#datadict[ll + 'Greeks'].impliedVol
                    tempdict[ll + '_delta'] = 0#datadict[ll + 'Greeks'].delta
                    tempdict[ll + '_optPrice'] = 0#datadict[ll + 'Greeks'].optPrice
                    tempdict[ll + '_gamma'] = 0#datadict[ll + 'Greeks'].gamma
                    tempdict[ll + '_vega'] = 0#datadict[ll + 'Greeks'].vega
                    tempdict[ll + '_theta'] = 0#datadict[ll + 'Greeks'].theta
                else:
                    #rint(datadict[ll+'Greeks'])
                    tempdict[ll + '_undPrice'] = datadict[ll+'Greeks'].undPrice
                    tempdict[ll + '_tickAttrib'] = datadict[ll+'Greeks'].tickAttrib
                    tempdict[ll + '_impliedVol'] = datadict[ll+'Greeks'].impliedVol
                    tempdict[ll + '_delta'] = datadict[ll+'Greeks'].delta
                    tempdict[ll + '_optPrice'] = datadict[ll+'Greeks'].optPrice
                    tempdict[ll + '_gamma'] = datadict[ll+'Greeks'].gamma
                    tempdict[ll + '_vega'] = datadict[ll+'Greeks'].vega
                    tempdict[ll + '_theta'] = datadict[ll+'Greeks'].theta

            if outdf.empty:
                print("Creating outdf")
                outdf = pd.DataFrame(tempdict, index=[counter])
            else:
                tempdf = pd.DataFrame(tempdict, index=[counter])
                outdf = pd.concat([outdf, tempdf], ignore_index=True)

            counter+=1

        outdf['iVol'] = (outdf['bid_impliedVol']+outdf['ask_impliedVol'])/2

        calls = outdf.loc[outdf['putcall'] == 'C']
        calls['mid_impliedVol'] = (calls['bid_impliedVol']+calls['ask_impliedVol'])/2
        calls['mid_delta'] = (calls['bid_delta']+calls['ask_delta'])/2
        calls['mid_theta'] = (calls['bid_theta']+calls['ask_theta'])/2
        calls['mid_gamma'] = (calls['bid_gamma']+calls['ask_gamma'])/2
        calls['mid_vega'] = (calls['bid_vega']+calls['ask_vega'])/2

        puts = outdf.loc[outdf['putcall'] == 'P']
        puts['mid_impliedVol'] = (puts['bid_impliedVol']+puts['ask_impliedVol'])/2
        puts['mid_delta'] = (puts['bid_delta']+puts['ask_delta'])/2
        puts['mid_theta'] = (puts['bid_theta']+puts['ask_theta'])/2
        puts['mid_gamma'] = (puts['bid_gamma']+puts['ask_gamma'])/2
        puts['mid_vega'] = (puts['bid_vega']+puts['ask_vega'])/2

        print('------------------------------CALLS-----------------------------')
        print(calls.head(30).to_string())
        #print(calls.sort_values(by='delta',ascending='False')
        print(calls[['time','strike','putcall','bid','ask','last_impliedVol','last_delta','last_theta','last_gamma','last_vega']].loc[calls['last_delta'] > 0.05].sort_values(by='strike',ascending=False).to_string())
        #print(calls.loc[calls['mid_delta'] > 0.05].to_string())
        print('------------------------------PUTS-----------------------------')
        print(puts[['time','strike','putcall','bid','ask','mid_impliedVol','mid_delta','mid_theta','mid_gamma','mid_vega']].loc[puts['mid_delta'] < -0.05].sort_values(by='strike').to_string())

        print('Call delta range:')
        print(calls['last_delta'].max(),calls['last_delta'].min())
        print('Put delta range:')
        print(puts['last_delta'].max(),puts['last_delta'].min())
        #print(outdf[['time','strike','putcall','bid','bidSize','last','lastSize','bid_impliedVol','bid_delta','model_delta']].to_string())
        outdf.to_csv(file+outdf['time'].iloc[0]+'.csv')
        ib.sleep(20)
    except Exception as e:
        print(e)


exit()

contract = Contract(symbol='ES',secType='FOP',lastTradeDateOrContractMonth=expiration,strike=strike,right=right,currency='USD',exchange='CME',tradingClass=c.tradingClass)

contract = Contract(secType='FOP', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231027', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='EW4')
ib.qualifyContracts(contract)

print(contract)

exit()


req_fields = ['time','bid','bidSize','ask','askSize','last','lastSize','prevBid','prevBidSize','prevAsk','prevAskSize','prevLast','prevLastSize','volume','open','high','low','close']

with open(file_dir+file, "a", newline="") as filetowrite:
    writer = csv.writer(filetowrite, delimiter=',')
    writer.writerow(req_fields)


    stop_trigger = False

    while not stop_trigger:
        with open(file_dir + file, "a", newline="") as filetowrite:
            writer = csv.writer(filetowrite, delimiter=',')


            ticks = ib.reqMktData(contract,"",True,False)

            ib.sleep(0.1)
            print('TIME: ',ticks.time)
            print('BID PRICE/SIZE: ',ticks.bid,' ',ticks.bidSize,' BID ASK/SIZE: ',ticks.ask,' ',ticks.askSize, ' BID/ASK SPREAD: ', ticks.ask-ticks.bid)

            #print(ticks.time,ticks.bid,ticks.bidSize,ticks.ask,ticks.askSize,ticks.last,ticks.lastSize,ticks.prevBid,ticks.prevBidSize,ticks.prevAsk,ticks.prevAskSize,ticks.prevLast,ticks.prevLastSize,ticks.volume,ticks.open,ticks.high,ticks.low,ticks.close)
            writer.writerow(
                [ticks.time,ticks.bid,ticks.bidSize,ticks.ask,ticks.askSize,ticks.last,ticks.lastSize,ticks.prevBid,ticks.prevBidSize,ticks.prevAsk,ticks.prevAskSize,ticks.prevLast,ticks.prevLastSize,ticks.volume,ticks.open,ticks.high,ticks.low,ticks.close])

        ib.sleep(5)


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

#input_string = ""
#while input_string != "stop":
#    ticker.updateEvent += onTickerUpdate

#    ib.sleep(15)
    #input_string = input("Type stop to stop")


#ib.cancelMktDepth(contract)
#ib.disconnect()

####

def onBarUpdate(bars, hasNewBar):
    print(bars[-1])
    bardict = bars[-1].dict()
    writer.writerow([bardict['time'],bardict['endTime'],bardict['open_'],bardict['high'],bardict['low'],bardict['close'],bardict['volume'],bardict['wap'],bardict['count']])

#req_fields = ['timestamp','endtime','open','high','low','close','volume','wap','count']

#with open("output.csv", "a", newline="") as f_output:
#    csv_output = csv.DictWriter(f_output, fieldnames=req_fields, extrasaction="ignore")
##    csv_output.writeheader()        # only needed once
 #   csv_output.writerow(data['data'][0]['content'][0])
##
#with open(file_dir+file, "a", newline="") as filetowrite:
#    writer = csv.writer(filetowrite, delimiter=',')
#    writer.writerow(req_fields)

#    bars = ib.reqRealTimeBars(contract, 5, 'TRADES', False)
#    bars.updateEvent += onBarUpdate
#    ib.sleep(60*60*60)

#ib.cancelRealTimeBars(bars)
