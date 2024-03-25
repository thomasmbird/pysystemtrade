import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
import numpy as np
import sys
from pathlib import Path
from contextlib import contextmanager

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
        self.includeExpired = False
    def qual_contr(self):

        ib.qualifyContracts(self)
        print('Successfully qualified ',self.localSymbol)

    #def localSymbol(self):
    #    return self.localSymbol
    def onBarUpdate(self, bars, hasNewBar):
        with open(file_dir + self.localSymbol + '/' + file, "a", newline="") as filetowrite:
            writer_internal = csv.writer(filetowrite, delimiter=',')
            print(self.localSymbol,bars[-1].tuple())
            bardict = bars[-1].dict()
            writer_internal.writerow(
                [bardict['time'], bardict['endTime'], bardict['open_'], bardict['high'], bardict['low'], bardict['close'],
                 bardict['volume'], bardict['wap'], bardict['count']])

    def stream_bars(self):
        print('Starting to stream ', self.localSymbol)
        if not os.path.exists(file_dir+self.localSymbol+'/'):
            os.makedirs(file_dir+self.localSymbol+'/')
            print('Made directory for contract: ', self.localSymbol)

        req_fields = ['timestamp', 'endtime', 'open', 'high', 'low', 'close', 'volume', 'wap', 'count']

        with open(file_dir+self.localSymbol+'/'+file, "w", newline="") as filetowrite:
            writer = csv.writer(filetowrite, delimiter=',')
            writer.writerow(req_fields)

            bars = ib.reqRealTimeBars(self, 5, 'TRADES', False)
            bars.updateEvent += self.onBarUpdate



        print('Successfully streaming ', self.localSymbol)

def myFunc(e):
    return e.lastTradeDateOrContractMonth

class FixSizedDict(dict):
    def __init__(self, *args, maxlen=0, **kwargs):
        self._maxlen = maxlen
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if self._maxlen > 0:
            if len(self) > self._maxlen:
                self.pop(next(iter(self)))

@contextmanager
def custom_redirection(fileobj):
    old = sys.stdout
    sys.stdout = fileobj
    try:
        yield fileobj
    finally:
        sys.stdout = old


ib = IB()
ib.connect('127.0.0.1', 4001, clientId=80)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
temp_dir = file_dir+'/ES/HistoricalOptions/'
file = 'ES_contracts.csv'
start_ind = 0
log_file_dir = file_dir+'HistDataLogs/ES_ticks/'


underlyings_df = pd.read_csv(file_dir+file)
#symbol_df = symbol_df.iloc[start_ind::]
#
# contract_object_list = []
# for ix in range(0, underlyings_df.shape[0]):
#     contract_object_list.append(IBFutures(str(symbol_df['conId'].iloc[ix]),symbol_df['symbol'].iloc[ix],symbol_df['exchange'].iloc[ix],'FUT',
#                                           symbol_df['lastTradeDateOrContractMonth'].iloc[ix],symbol_df['currency'].iloc[ix]))
#
#
#
# print(underlyings_df)
# for ix in range(0,underlyings_df.shape[0]):
#     localsymb = underlyings_df['localSymbol'].iloc[ix]
#     expdate = underlyings_df['lastTradeDateOrContractMonth'].iloc[ix]
#     print(localsymb,expdate)
und_contract = Contract(secType='FUT',symbol='ES',currency='USD',localSymbol='ESZ3',#lastTradeDateOrContractMonth=expdate,
                            exchange='CME')

print(ib.qualifyContracts(und_contract))
chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId) #includeExpired=True)
chainsdf = util.df(chains)
print(chainsdf.sort_values(by='expirations').to_string())

exp_list = chainsdf['expirations'].sort_values()
#print(exp_list)
#print(util.df(chains).to_string())

es_price = 4508.0



#for exp in exp_list:
#    print(exp[0])
expiry = '20231215'
#expiry = exp_list[0][0]
#print(expiry)
#exit()
for c in chains:
    if expiry in c.expirations:
        strikes = c.strikes
        tradingClass = c.tradingClass
        undconid = c.underlyingConId
        exch = c.exchange
        mult = c.multiplier

strikes = [strike for strike in strikes if strike > 4000 and strike < 5000]

print(len(strikes))

starting_strike = min(strikes, key=lambda x:abs(x-es_price))
rights = ['C', 'P']

strikes.sort(key=lambda x:abs(x-es_price))

contracts_init = [Contract(symbol='ES', secType='FOP', multiplier = 50, lastTradeDateOrContractMonth=expiry, strike=strike, right=right, exchange='CME', currency='USD', tradingClass=tradingClass)
            for strike in strikes
            for right in rights]

contracts = []
for contr in contracts_init:
    if contr.right == 'P':
        if contr.strike < 4600:
            contracts.append(contr)
    elif contr.right == 'C':
        if contr.strike > 4200:
            contracts.append(contr)

print(len(contracts))
print(contracts)

#_first started at 00:00
#_second started at 12:00


date_list = ['20231117 12:00:00 US/Central',
             '20231117 06:00:00 US/Central',
             '20231117 00:00:00 US/Central',
             '20231116 18:00:00 US/Central',
             '20231116 12:00:00 US/Central',
             '20231116 06:00:00 US/Central',
             '20231116 00:00:00 US/Central',
             '20231115 18:00:00 US/Central',
             '20231115 12:00:00 US/Central',
             '20231115 06:00:00 US/Central',
             '20231115 00:00:00 US/Central',
             '20231114 18:00:00 US/Central',
             '20231114 12:00:00 US/Central',
             '20231114 06:00:00 US/Central',
             '20231114 00:00:00 US/Central']

             #
             # '20231115 12:00:00 US/Central',
             # '20231114 12:00:00 US/Central',
             # '20231111 12:00:00 US/Central',
             # '20231110 12:00:00 US/Central',
             # '20231109 12:00:00 US/Central',
             # '20231108 12:00:00 US/Central',
             # '20231107 12:00:00 US/Central']

#date_list = ['20231117 00:00:00 US/Central']

ctr = 0
for contract in contracts:
    with open(log_file_dir + contract.localSymbol + '.txt', 'a') as out:

        try:
            print(ib.qualifyContracts(contract))
        except Exception as e:
            print("Failed to qualify contract: ", e)

        for date in date_list:

            st = tyme.time()
            try:
                bars = ib.reqHistoricalTicks(
                    contract,
                    startDateTime='',
                    endDateTime=date,
                    numberOfTicks=1000,
                    whatToShow='TRADES',
                    useRth=0)

                print(contract.localSymbol)
                print(len(bars))
                print(bars[0].time)
                print(bars[-1].time)

                ticksdf = pd.DataFrame(columns=['contract','time', 'price', 'size'])
                for iy in range(0, len(bars)):
                    # print(bars[iy].dict())
                    ticksdf.loc[iy] = [contract.localSymbol,bars[iy].time.strftime("%Y-%m-%d %H:%M%S"),bars[iy].price,bars[iy].size]
                print('Request took: ',tyme.time()-st)
            except Exception as e:
                with custom_redirection(out):
                    print(contract.localSymbol,date)
                    print(e)

            print(ctr)
            ctr += 1
            ib.sleep(0.2)

            ticksdf.to_csv(temp_dir+contract.localSymbol+'ticks.csv',mode='a',header=not os.path.exists(temp_dir+contract.localSymbol+'ticks.csv'))

exit()


