import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
import numpy as np
from arctic import Arctic
from arctic import TICK_STORE
import time as tyme


pd.options.mode.chained_assignment = None
ib = IB()
ib.connect('127.0.0.1', 4001, clientId=22)

store = Arctic('localhost')
store.initialize_library('ES_0dte_options', lib_type=TICK_STORE)
lib = store['ES_0dte_options']

file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/Options/'
file_name_base = '0dte_'

today = datetime.datetime.today().strftime('%m_%d_%y')
tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

if datetime.datetime.today().hour > 16:
    log_date = tomorrow
else:
    log_date = today
print('Choosing log date: ',log_date)

file = file_dir+log_date+file_name_base

und_contract = Contract(secType='FUT',symbol='ES',currency='USD',localSymbol='ESZ3',exchange='CME')
ib.qualifyContracts(und_contract)
chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId)

#expiry='20231116'

#file = file+'_'+expiry+'_'
#expiries = ['20231117','20231120','20231121','20231122','20231123'],#'20231124']
expiry = '20231117'
for c in chains:

    if expiry in c.expirations:
        strikes = c.strikes
        tradingClass = c.tradingClass
        undconid = c.underlyingConId
        exch = c.exchange
        mult = c.multiplier

es_price = 4513

strikes = [strike for strike in strikes if strike > 4300 and strike < 4700]

rights = ['C', 'P']

contracts_init = [Contract(symbol='ES', secType='FOP', multiplier = 50, lastTradeDateOrContractMonth=expiry, strike=strike, right=right, exchange='CME', currency='USD', tradingClass=tradingClass)
            for strike in strikes
            for right in rights]
            #for expiry in expiries]

contracts = []
for contr in contracts_init:
    if contr.right == 'P':
        if contr.strike < 4550:
            contracts.append(contr)
    elif contr.right == 'C':
        if contr.strike > 4450:
            contracts.append(contr)

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
            ib.sleep(0.05)
            outputlist.append([x.strike, x.right, snapshot])
        tradingClass = x.tradingClass
        expdate = x.lastTradeDateOrContractMonth
        print('Time elapsed calling reqMktData: ', tyme.time() - st)
        ib.sleep(2)
        ctr = 0
        st=tyme.time()
        #while util.isNan(outputlist[0][2].bid) or util.isNan(outputlist[-1][2].bid):
        while any([util.isNan(item[2].bid) for item in outputlist]):
            print('WAITING')

            print(ctr)
            ib.sleep(1)
            ctr += 1

        print('Time elapsed waiting for reqMktData: ', tyme.time() - st)
        print([item[2].bid for item in outputlist])
        st = tyme.time()


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
            tempdict['timestamp'] = datadict['time']
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

        outdf.drop(columns=['timestamp']).to_csv(file+outdf['time'].iloc[0]+'.csv')

        outdf = outdf.rename(columns={'timestamp': 'index'})
        outdf = outdf.fillna('nan')
        #dictlist = []
        for ix in range(0,outdf.shape[0]):
            dict = {}

            for col in outdf.columns:
                if col == "index":
                    dict[col] = outdf[col].iloc[ix].to_pydatetime()
                else:
                    dict[col] = outdf[col].iloc[ix]

            try:

                lib.write("{}_{}_{}_{}_top of book".format(tradingClass,expdate,str(dict['strike']),dict['putcall']),
                          [dict])
            except Exception as e:
                print(e)

        if datetime.datetime.now().hour < 8:
            ib.sleep(60)
        else:
            ib.sleep(10)
    except Exception as e:
        print(e)


exit()
