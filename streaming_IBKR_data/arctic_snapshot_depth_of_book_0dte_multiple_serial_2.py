import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
import numpy as np
from arctic import Arctic
from arctic import TICK_STORE
from multiprocessing import Process
import time as tyme

class SetofContracts():
    def __init__(self, clientId, contracts, undsymbol:str, expiration: str, localSymbol: str, output_file_prefix: str):
        self.clientId = clientId
        self.contracts = contracts
        self.undsymbol = undsymbol
        self.expiration = expiration
        self.localSymbol = localSymbol
        self.output_file_prefix = output_file_prefix
        #internal_ib = IB()
        print("Connecting to ib with id: ", self.clientId)
        #self.ib = internal_ib.connect('127.0.0.1', 4001, clientId=self.clientId)
    def qual_contr(self):
        internal_ib = IB()
        internal_ib.connect('127.0.0.1', 4001, clientId=self.clientId)
        print("Connected to IB with id: ", self.clientId)
        for ctr in self.contracts:
            print(internal_ib.qualifyContracts(ctr))
        print('Finished qualifying contracts for id: ',self.clientId)
        internal_ib.disconnect()


    def snap_top_of_book(self):
        internal_ib = IB()
        internal_ib.connect('127.0.0.1', 4001, clientId=self.clientId)
        print("Connected to IB with id: ", self.clientId)
        for ctr in self.contracts:
            print(internal_ib.qualifyContracts(ctr))
        print('Finished qualifying contracts for id: ', self.clientId)
        #internal_ib.disconnect()

        store = Arctic('localhost')
        lib = store['ES_full_optionchain']


        self.output_file_prefix = self.output_file_prefix + self.contracts[0].localSymbol.split(' ')[0] + '_' + self.contracts[
            0].lastTradeDateOrContractMonth

        temp_var = False
        if temp_var:
            tyme.sleep(5)
            print(self.clientId, "working as intended, done")
            internal_ib.disconnect()
            return 0



        stop_flag = False
        df = pd.DataFrame()
        tickerlist = []
        curr_hour = 18
        while curr_hour != 16:
        #while not stop_flag:
            try:
                st = tyme.time()
                outputlist = []
                curr_hour = datetime.datetime.now().hour
                file = self.output_file_prefix + '_' + str(datetime.datetime.now().hour) + '_' 'opt_tob.csv'

                for x in self.contracts:
                    snapshot = internal_ib.reqMktData(x, "", True, False)
                    internal_ib.sleep(0.05)
                    outputlist.append([x.strike, x.right, snapshot])

                tradingClass = x.tradingClass
                expdate = x.lastTradeDateOrContractMonth
                localSym = x.localSymbol.split(" ")[0]
                print(self.clientId,' Time elapsed calling reqMktData: ', tyme.time() - st)
                internal_ib.sleep(2)
                ctr = 0
                st = tyme.time()
                # while util.isNan(outputlist[0][2].bid) or util.isNan(outputlist[-1][2].bid):
                while any([util.isNan(item[2].ask) for item in outputlist]):
                    print(self.clientId,' WAITING')
                    print(ctr)
                    print([item[2].bid for item in outputlist])
                    print([item[2].ask for item in outputlist])
                    internal_ib.sleep(1)
                    ctr += 1
                    if ctr == 100:
                        assert False

                print(self.clientId, ' Time elapsed waiting for reqMktData: ', tyme.time() - st)
                print(self.clientId, [item[2].bid for item in outputlist])
                st = tyme.time()

                outdf = pd.DataFrame()

                counter = 0
                for output in outputlist:
                    tempdict = {}
                    datadict = output[2].dict()

                    ##print(datadict['bidGreeks'])
                    # print(datadict['askGreeks'])
                    ##print(datadict['lastGreeks'])
                    # print(datadict['modelGreeks'])
                    # print(datadict.keys())
                    # print(datadict.values())
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
                    # tempdict['undPrice'] = datadict['bidGreeks'].undPrice
                    # tempdict['undPrice2'] = datadict['askGreeks'].undPrice
                    # print('here')
                    listy = ['bid', 'ask', 'last', 'model']
                    for ll in listy:
                        # if datadict[ll+'Greeks']
                        if datadict[ll + 'Greeks'] == None:
                            tempdict[ll + '_undPrice'] = 0
                            tempdict[ll + '_tickAttrib'] = 0
                            tempdict[ll + '_impliedVol'] = 0  # datadict[ll + 'Greeks'].impliedVol
                            tempdict[ll + '_delta'] = 0  # datadict[ll + 'Greeks'].delta
                            tempdict[ll + '_optPrice'] = 0  # datadict[ll + 'Greeks'].optPrice
                            tempdict[ll + '_gamma'] = 0  # datadict[ll + 'Greeks'].gamma
                            tempdict[ll + '_vega'] = 0  # datadict[ll + 'Greeks'].vega
                            tempdict[ll + '_theta'] = 0  # datadict[ll + 'Greeks'].theta
                        else:
                            # rint(datadict[ll+'Greeks'])
                            tempdict[ll + '_undPrice'] = datadict[ll + 'Greeks'].undPrice
                            tempdict[ll + '_tickAttrib'] = datadict[ll + 'Greeks'].tickAttrib
                            tempdict[ll + '_impliedVol'] = datadict[ll + 'Greeks'].impliedVol
                            tempdict[ll + '_delta'] = datadict[ll + 'Greeks'].delta
                            tempdict[ll + '_optPrice'] = datadict[ll + 'Greeks'].optPrice
                            tempdict[ll + '_gamma'] = datadict[ll + 'Greeks'].gamma
                            tempdict[ll + '_vega'] = datadict[ll + 'Greeks'].vega
                            tempdict[ll + '_theta'] = datadict[ll + 'Greeks'].theta

                    if outdf.empty:
                        print(self.clientId, " Creating outdf")
                        outdf = pd.DataFrame(tempdict, index=[counter])
                    else:
                        tempdf = pd.DataFrame(tempdict, index=[counter])
                        outdf = pd.concat([outdf, tempdf], ignore_index=True)

                    counter += 1

                outdf['iVol'] = (outdf['bid_impliedVol'] + outdf['ask_impliedVol']) / 2

                calls = outdf.loc[outdf['putcall'] == 'C'].sort_values(by=['strike'], ascending=False)
                puts = outdf.loc[outdf['putcall'] == 'P'].sort_values(by=['strike'])
                print(calls[['time', 'strike', 'putcall', 'bidSize', 'bid', 'ask', 'askSize', 'last', 'ask_delta',
                             'ask_theta', 'ask_gamma', 'ask_vega', 'ask_impliedVol']].loc[calls['ask_delta'] > 0.05].iloc[0:10,:].to_string())
                print(puts[['time', 'strike', 'putcall', 'bidSize', 'bid', 'ask', 'askSize', 'last', 'ask_delta',
                            'ask_theta',
                            'ask_gamma', 'ask_vega', 'ask_impliedVol']].loc[puts['ask_delta'] < -0.05].iloc[-10::,:].to_string())

                print('Call Range: ', calls['ask_delta'].min(),calls['ask_delta'].max())
                print('Put Range: ', puts['ask_delta'].min(), puts['ask_delta'].max())






                outdf.drop(columns=['timestamp']).to_csv(file, mode='a', header=not os.path.exists(file))
                # outdf.drop(columns=['timestamp']).to_csv(file+outdf['time'].iloc[0]+'.csv')

                #FOR WRITIGN TO ARCTIC, SKIP FOR NOW
                outdf = outdf.rename(columns={'timestamp': 'index'})
                outdf = outdf.fillna('nan')
                outdf['index'] = outdf['index'].dt.to_pydatetime()
                outdf = outdf.sort_values(by=['index'])
                #print(outdf.to_dict('records'))
                #exit()
                try:
                    lib.write("{}_{}_top of book".format(localSym, expdate), outdf.to_dict('records'))
                except Exception as e:
                    print(e)
                #
                # for ix in range(0, outdf.shape[0]):
                #     dict = {}
                #     for col in outdf.columns:
                #         if col == "index":
                #             dict[col] = outdf[col].iloc[ix].to_pydatetime()
                #         else:
                #             dict[col] = outdf[col].iloc[ix]
                #     try:
                #         lib.write("{}_{}_top of book".format(tradingClass, expdate),
                #                   [dict])
                #     except Exception as e:
                #         print(e)

                print(self.clientId, ' Time elapsed for full loop: ', tyme.time() - st)
                if datetime.datetime.now().hour < 8 or datetime.datetime.now().hour > 16:
                    print(self.clientId, "Time is: ",datetime.datetime.now().hour," so sleeping for 60s")
                    internal_ib.sleep(60)
                else:
                    print(self.clientId, "Time is: ", datetime.datetime.now().hour, " so sleeping for 5s")
                    internal_ib.sleep(5)
            except Exception as e:
                print(e)

        print(self.clientId, " Hit some sort of error, exiting.")
        internal_ib.disconnect()

        return 0


def start_run(run_id,obj):
    print("Starting Obj Run ", run_id)
    #obj.qual_contr()
    obj.snap_top_of_book()
    print("Finishing obj run ", run_id)
    return 0


if __name__ == '__main__':
    pd.options.mode.chained_assignment = None
    start_id = 80
    #print(start_id)
    ib = IB()
    ib.connect('127.0.0.1', 4001, clientId=start_id)

    #tore = Arctic('localhost')
    #store.initialize_library('ES_full_optionchain', lib_type=TICK_STORE)
    #lib = store['ES_full_optionchain']

    file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/OptionsTest/'
    file_name_base = 'opt_tob.csv'

    today = datetime.datetime.today().strftime('%m_%d_%y')
    tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

    if datetime.datetime.today().hour > 16:
        log_date = tomorrow
    else:
        log_date = today
    print('Choosing log date: ', log_date)

    file = file_dir+log_date+'/'
    if not os.path.exists(file):
        os.makedirs(file)
        print('Made directory for date: ', log_date)

    base_file = file
    und_contract = Contract(secType='FUT',symbol='ES',currency='USD',localSymbol='ESH4',exchange='CME')
    ib.qualifyContracts(und_contract)
    chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId)

    #print(util.df(chains).sort_values(by=['expirations']).to_string())
    #exit()

    #expiry = '20231120'
    expiry_list = ['20240205','20240206','20240207','20240208','20240209']

    es_price = 4900


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
    strike_dict['Max'] = es_price+200
    strike_dict['Min'] = es_price-200
    strike_dict['PutMax'] = es_price+50
    strike_dict['CallMin'] = es_price-50

    contracts_list = []
    for expiry in expiry_list:
        contracts_list.append(get_contracts(ib, expiry, chains, strike_dict))

    obj_list = []
    ctr = 1
    for expiry, contrlist in zip(expiry_list,contracts_list):
        #obj_list.append(SetofContracts(clientId=start_id+ctr, contracts=contrlist, undsymbol='ES', expiration=expiry, localSymbol=contrlist[0].localSymbol,
        #                               output_file_prefix=file))
        obj_list.append(SetofContracts(clientId=start_id, contracts=contrlist, undsymbol='ES', expiration=expiry,
                                       localSymbol=contrlist[0].localSymbol,output_file_prefix=file))
        for contr in contrlist:
            print(ib.qualifyContracts(contr))

        print('Finished qualifying contracts for id: ', ctr)
        ctr += 1




    store = Arctic('localhost')
    lib = store['ES_full_optionchain']

    #output_file_prefix = output_file_prefix + contracts[0].localSymbol.split(' ')[0] + '_' + contracts[0].lastTradeDateOrContractMonth

    stop_flag = False
    df = pd.DataFrame()
    tickerlist = []
    curr_hour = 18
    while curr_hour != 16:
        outctr = 0
        for obj in obj_list:
        # while not stop_flag:
            try:
                st = tyme.time()
                outputlist = []
                curr_hour = datetime.datetime.now().hour

                contracts = obj.contracts

                output_file_prefix = base_file + contracts[0].tradingClass.split(' ')[0] + '_' + contracts[
                    0].lastTradeDateOrContractMonth

                file = output_file_prefix + '_' + str(datetime.datetime.now().hour) + '_' 'opt_tob.csv'

                for x in contracts:
                    snapshot = ib.reqMktData(x, "", True, False)
                    ib.sleep(0.05)
                    outputlist.append([x.strike, x.right, snapshot])

                tradingClass = x.tradingClass
                expdate = x.lastTradeDateOrContractMonth
                localSym = x.localSymbol.split(" ")[0]
                print(outctr, ' Time elapsed calling reqMktData: ', tyme.time() - st)
                ib.sleep(2)
                ctr = 0
                st = tyme.time()
                # while util.isNan(outputlist[0][2].bid) or util.isNan(outputlist[-1][2].bid):
                while any([util.isNan(item[2].ask) for item in outputlist]):
                    print(ctr, ' WAITING')
                    print(ctr)
                    print([item[2].bid for item in outputlist])
                    print([item[2].ask for item in outputlist])
                    ib.sleep(1)
                    ctr += 1
                    if ctr == 100:
                        assert False

                print(outctr, ' Time elapsed waiting for reqMktData: ', tyme.time() - st)
                print(outctr, [item[2].bid for item in outputlist])
                st = tyme.time()

                outdf = pd.DataFrame()

                counter = 0
                for output in outputlist:
                    tempdict = {}
                    datadict = output[2].dict()

                    ##print(datadict['bidGreeks'])
                    # print(datadict['askGreeks'])
                    ##print(datadict['lastGreeks'])
                    # print(datadict['modelGreeks'])
                    # print(datadict.keys())
                    # print(datadict.values())
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
                    # tempdict['undPrice'] = datadict['bidGreeks'].undPrice
                    # tempdict['undPrice2'] = datadict['askGreeks'].undPrice
                    # print('here')
                    listy = ['bid', 'ask', 'last', 'model']
                    for ll in listy:
                        # if datadict[ll+'Greeks']
                        if datadict[ll + 'Greeks'] == None:
                            tempdict[ll + '_undPrice'] = 0
                            tempdict[ll + '_tickAttrib'] = 0
                            tempdict[ll + '_impliedVol'] = 0  # datadict[ll + 'Greeks'].impliedVol
                            tempdict[ll + '_delta'] = 0  # datadict[ll + 'Greeks'].delta
                            tempdict[ll + '_optPrice'] = 0  # datadict[ll + 'Greeks'].optPrice
                            tempdict[ll + '_gamma'] = 0  # datadict[ll + 'Greeks'].gamma
                            tempdict[ll + '_vega'] = 0  # datadict[ll + 'Greeks'].vega
                            tempdict[ll + '_theta'] = 0  # datadict[ll + 'Greeks'].theta
                        else:
                            # rint(datadict[ll+'Greeks'])
                            tempdict[ll + '_undPrice'] = datadict[ll + 'Greeks'].undPrice
                            tempdict[ll + '_tickAttrib'] = datadict[ll + 'Greeks'].tickAttrib
                            tempdict[ll + '_impliedVol'] = datadict[ll + 'Greeks'].impliedVol
                            tempdict[ll + '_delta'] = datadict[ll + 'Greeks'].delta
                            tempdict[ll + '_optPrice'] = datadict[ll + 'Greeks'].optPrice
                            tempdict[ll + '_gamma'] = datadict[ll + 'Greeks'].gamma
                            tempdict[ll + '_vega'] = datadict[ll + 'Greeks'].vega
                            tempdict[ll + '_theta'] = datadict[ll + 'Greeks'].theta

                    if outdf.empty:
                        print(counter, " Creating outdf")
                        outdf = pd.DataFrame(tempdict, index=[counter])
                    else:
                        tempdf = pd.DataFrame(tempdict, index=[counter])
                        outdf = pd.concat([outdf, tempdf], ignore_index=True)

                    counter += 1

                outdf['iVol'] = (outdf['bid_impliedVol'] + outdf['ask_impliedVol']) / 2

                calls = outdf.loc[outdf['putcall'] == 'C'].sort_values(by=['strike'], ascending=False)
                puts = outdf.loc[outdf['putcall'] == 'P'].sort_values(by=['strike'])
                print(calls[['time', 'strike', 'putcall', 'bidSize', 'bid', 'ask', 'askSize', 'last', 'ask_delta',
                             'ask_theta', 'ask_gamma', 'ask_vega', 'ask_impliedVol']].loc[calls['ask_delta'] > 0.05].iloc[
                      0:10, :].to_string())
                print(puts[['time', 'strike', 'putcall', 'bidSize', 'bid', 'ask', 'askSize', 'last', 'ask_delta',
                            'ask_theta',
                            'ask_gamma', 'ask_vega', 'ask_impliedVol']].loc[puts['ask_delta'] < -0.05].iloc[-10::,
                      :].to_string())

                print('Call Range: ', calls['ask_delta'].min(), calls['ask_delta'].max())
                print('Put Range: ', puts['ask_delta'].min(), puts['ask_delta'].max())

                outdf.drop(columns=['timestamp']).to_csv(file, mode='a', header=not os.path.exists(file))
                # outdf.drop(columns=['timestamp']).to_csv(file+outdf['time'].iloc[0]+'.csv')

                # FOR WRITIGN TO ARCTIC, SKIP FOR NOW
                outdf = outdf.rename(columns={'timestamp': 'index'})
                outdf = outdf.fillna('nan')
                outdf['index'] = outdf['index'].dt.to_pydatetime()
                outdf = outdf.sort_values(by=['index'])
                # print(outdf.to_dict('records'))
                # exit()
                try:
                    lib.write("{}_{}_top of book".format(localSym, expdate), outdf.to_dict('records'))
                except Exception as e:
                    print(e)
                #
                # for ix in range(0, outdf.shape[0]):
                #     dict = {}
                #     for col in outdf.columns:
                #         if col == "index":
                #             dict[col] = outdf[col].iloc[ix].to_pydatetime()
                #         else:
                #             dict[col] = outdf[col].iloc[ix]
                #     try:
                #         lib.write("{}_{}_top of book".format(tradingClass, expdate),
                #                   [dict])
                #     except Exception as e:
                #         print(e)

                print(outctr, ' Time elapsed for full loop: ', tyme.time() - st)
            except Exception as e:
                print(e)
            outctr += 1
        if datetime.datetime.now().hour < 8 or datetime.datetime.now().hour > 16:
            print("Time is: ", datetime.datetime.now().hour, " so sleeping for 60s")
            ib.sleep(60)
        else:
            print("Time is: ", datetime.datetime.now().hour, " so sleeping for 5s")
            ib.sleep(5)


    print(" Hit some sort of error, exiting.")
    ib.disconnect()
