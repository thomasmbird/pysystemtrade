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
file_name_base = 'opt_tob.csv'

today = datetime.datetime.today().strftime('%m_%d_%y')
tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

if datetime.datetime.today().hour > 16:
    log_date = tomorrow
else:
    log_date = today
print('Choosing log date: ',log_date)

file = file_dir+log_date+'/'
if not os.path.exists(file):
    os.makedirs(file)
    print('Made directory for date: ',log_date)

und_contract = Contract(secType='FUT',symbol='ES',currency='USD',localSymbol='ESZ3',exchange='CME')
ib.qualifyContracts(und_contract)
chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId)

expiry = '20231121'

es_price = 4563

def get_contracts(client,expiry,chains,strike_limits):

    for c in chains:
        if expiry in c.expirations:
            strikes = c.strikes
            tradingClass = c.tradingClass
            undconid = c.underlyingConId
            exch = c.exchange
            mult = c.multiplier

    strikes = [strike for strike in strikes if strike > strike_limits['Min'] and strike < strike_limits['Max']]
    rights = ['C', 'P']

    contracts_init = [Contract(symbol='ES', secType='FOP', multiplier = 50, lastTradeDateOrContractMonth=expiry, strike=strike, right=right, exchange='CME', currency='USD', tradingClass=tradingClass)
                for strike in strikes
                for right in rights]

    contracts = []
    for contr in contracts_init:
        if contr.right == 'P':
            if contr.strike < strike_limits['PutMax']:
                contracts.append(contr)
        elif contr.right == 'C':
            if contr.strike > strike_limits['CallMin']:
                contracts.append(contr)

    return contracts

strike_dict = {}
strike_dict['Max'] = 4750
strike_dict['Min'] = 4300
strike_dict['PutMax'] = es_price+50
strike_dict['CallMin'] = es_price-50

contracts = get_contracts(ib, expiry, chains, strike_dict)

for contract in contracts:
    try:
        print(ib.qualifyContracts(contract))
        #if contract.localSymbol.split(' ')[0] != locsymb_base:
        #    print("PROBLEM!  CONTRACT HAS LOCALSYMB: ", contract.localSymbol," BUT WAS EXPECTING ",locsymb_base)
    except Exception as e:
        print("Failed to qualify contract: ", e)
        exit()
    #print(file + contract.localSymbol.split(' ')[0] + '_' + contract.lastTradeDateOrContractMonth + '_' + str(
    #    datetime.datetime.now().hour) + file_name_base)

file_prefix = file + contract.localSymbol.split(' ')[0] + '_' + contract.lastTradeDateOrContractMonth + '_'
#print(file+"_".join(contract.localSymbol.split(' '))+'_'+contract.lastTradeDateOrContractMonth+file_name_base)
#print(file+contract.localSymbol.split(' ')[0]+'_'+contract.lastTradeDateOrContractMonth+'_'+str(datetime.datetime.now().hour)+file_name_base)
print("File is now: ", file)

#file = file_dir+

stop_flag = False
df = pd.DataFrame()
tickerlist = []


while not stop_flag:
    try:
        st = tyme.time()
        outputlist = []

        file = file_prefix + '_' + str(datetime.datetime.now().hour) + '_' + file_name_base

        for x in contracts:
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
        while any([util.isNan(item[2].ask) for item in outputlist]):
            print('WAITING')
            print(ctr)
            print([item[2].bid for item in outputlist])
            print([item[2].ask for item in outputlist])
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

        calls = outdf.loc[outdf['putcall'] == 'C'].sort_values(by=['strike'], ascending=False)
        puts = outdf.loc[outdf['putcall'] == 'P'].sort_values(by=['strike'])
        print(calls[['time','strike','putcall','bidSize','bid','ask','askSize','last','bid_delta','bid_theta','bid_gamma','bid_vega','bid_impliedVol']].to_string(),
              puts[['time', 'strike', 'putcall', 'bidSize', 'bid', 'ask', 'askSize', 'last', 'bid_delta', 'bid_theta',
                     'bid_gamma', 'bid_vega', 'bid_impliedVol']].to_string())
        # print('------------------------------CALLS-----------------------------')
        # print(calls.head(30).to_string())
        # print(calls[['time','strike','putcall','bid','ask','last_impliedVol','last_delta','last_theta','last_gamma','last_vega']].loc[calls['last_delta'] > 0.05].sort_values(by='strike',ascending=False).to_string())
        # print(puts[['time','strike','putcall','bid','ask','mid_impliedVol','mid_delta','mid_theta','mid_gamma','mid_vega']].loc[puts['mid_delta'] < -0.05].sort_values(by='strike').to_string())
        #

        print('Call delta range:')
        print(calls['last_delta'].max(),calls['last_delta'].min())
        print('Put delta range:')
        print(puts['last_delta'].max(),puts['last_delta'].min())

        outdf.drop(columns=['timestamp']).to_csv(file, mode='a', header=not os.path.exists(file))
        #outdf.drop(columns=['timestamp']).to_csv(file+outdf['time'].iloc[0]+'.csv')

        outdf = outdf.rename(columns={'timestamp': 'index'})
        outdf = outdf.fillna('nan')

        print(outdf.to_dict('records'))

        #exit() #
        dictlist = []
        for ix in range(0,outdf.shape[0]):
            dict = {}

            for col in outdf.columns:
                if col == "index":
                    dict[col] = outdf[col].iloc[ix].to_pydatetime()
                else:
                    dict[col] = outdf[col].iloc[ix]

            try:
                print("{}_{}_{}_{}_top of book".format(tradingClass, expdate))
                print([dict])
                lib.write("{}_{}_{}_{}_top of book".format(tradingClass, expdate, str(dict['strike']), dict['putcall']),
                          [dict])
            except Exception as e:
                print(e)

        print('*************************************************************')
        print(datetime.datetime.now().hour)
        print('*************************************************************')
        if datetime.datetime.now().hour < 8 or datetime.datetime.now().hour > 16:
            ib.sleep(60)
        else:
            ib.sleep(10)
    except Exception as e:
        print(e)


exit()
