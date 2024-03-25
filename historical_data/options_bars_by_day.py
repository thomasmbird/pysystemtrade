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
log_file_dir = file_dir+'HistDataLogs/Options_ES/'


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
exit()
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

strikes = [strike for strike in strikes if strike > 4200 and strike < 4800]

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
        if contr.strike > 4450:
            contracts.append(contr)

print(len(contracts))
print(contracts)

for contract in contracts:
    print(contract.strike,contract.right)
    try:
        print(ib.qualifyContracts(contract))
    except Exception as e:
        print("Failed to qualify contract: ", e)
        exit()

    past_expiry = False

    vol_dict = FixSizedDict(maxlen=5)

    new_length = 1
    old_length = 0
    rolling_avg_volume = 10


    highest_counter = 0
    while new_length > old_length and rolling_avg_volume > 0.00005:
        print("Highest Lvl counter: ",highest_counter)
        with open(log_file_dir + contract.localSymbol + '.txt', 'a') as out:
            with custom_redirection(out):
                print("Highest Lvl counter: ", highest_counter)
            log_file = temp_dir + str(contract.localSymbol) + '_' + str(contract.lastTradeDateOrContractMonth) + '.csv'
            #print("Starting Loop")
            if os.path.isfile(log_file):
                barsdf = pd.read_csv(log_file, index_col=0)

                with custom_redirection(out):
                    start_time = barsdf['time'].iloc[0]
                    print("Loaded file with start_time: {}".format(start_time))


                month = start_time.split('/')[0]
                day = start_time.split('/')[1]
                year = start_time.split('/')[2].split(' ')[0]
                hour = start_time.split(' ')[1].split(':')[0]
                min = start_time.split(' ')[1].split(':')[1]
                sec = start_time.split(' ')[1].split(':')[2]

                req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                #req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Eastern'
                with custom_redirection(out):
                    print('Request starting at first bar from file: {}'.format(req_time))

            else:
                barsdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
                with custom_redirection(out):
                    print("No file to load, starting at present time")
                print("No file to load, starting at present time")

                with custom_redirection(out):
                    if past_expiry:
                        print("No file to load, past expiry so starting at: ", req_time)
                    else:
                        req_time = ''
                        newtime = datetime.datetime.now()-datetime.timedelta(days=1)

            counter = 0
            while counter < 5 and new_length != old_length and rolling_avg_volume > 0.00005:
                print("Starting counter loop: ",counter)
                with custom_redirection(out):
                    print("Starting counter loop: ", counter)
                if counter > 0:
                    start_time = str(barsdf['time'].iloc[0])
                    with custom_redirection(out):
                        print("Start time: ",start_time)
                        print("Counter: ",counter)
                    month = start_time.split('/')[0]
                    day = start_time.split('/')[1]
                    year = start_time.split('/')[2].split(' ')[0]
                    hour = start_time.split(' ')[1].split(':')[0]
                    min = start_time.split(' ')[1].split(':')[1]
                    sec = start_time.split(' ')[1].split(':')[2]

                    #req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                    req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                    with custom_redirection(out):
                        print('Inner-loop Request starting at: {}'.format(req_time))

                if len(req_time) != 0:
                     #print("Next datapoint should be: ")
                     time = datetime.datetime.strptime(req_time.split(' ')[0] + ' ' + req_time.split(' ')[1],
                                                       "%Y%m%d %H:%M:%S")
                     newtime = time - datetime.timedelta(days=1)
                    #         if newtime.hour == 15:
                    #             newtime = newtime - datetime.timedelta(days=1)
                     with custom_redirection(out):
                        print("Next timepoint should be: ",newtime.strftime("%m/%d/%Y %H:%M:%S"))

                old_length = barsdf.shape[0]
                bars = []
                print('----------------')
                print(req_time)
                print('----------------')
                with custom_redirection(out):
                    print('----------------')
                    print(req_time)
                    print('----------------')
                #with custom_redirection(out):
                bars = ib.reqHistoricalTicks(
                    contract,
                    startDateTime='',
                    endDateTime="20231114 16:00:00 US/Central",
                    numberOfTicks=500,
                    whatToShow='TRADES',
                    useRth=1)

                print(bars)
                print(len(bars))
                for item in bars:
                    #print(item)
                    print(item.time)
                    print(item.price,item.size)
                    #print(item.priceAsk,item.sizeAsk)
                exit()

                with custom_redirection(out):
                    print("Got past data call")
                if len(bars) == 0:
                    with custom_redirection(out):
                        print("No bars!  Didn't work.  Time to loop further back in time.")

                    subctr = 0
                    mult_fact = 1
                    while len(bars) == 0 and subctr < 10:
                        tyme.sleep(0.2)
                        with custom_redirection(out):
                            time = datetime.datetime.strptime(req_time.split(' ')[0]+' '+req_time.split(' ')[1], "%Y%m%d %H:%M:%S")
                            print(time)
                            newtime = time - datetime.timedelta(days=1)
                            print(newtime)

                            new_req_time = datetime.datetime.strftime(newtime,"%Y%m%d %H:%M:%S")
                            req_time = new_req_time+' '+req_time.split(' ')[2]

                            print("Trying {} minus 1 day".format(req_time))
                            #print(str(sec_start)+' S')
                            bars = ib.reqHistoricalData(
                                contract,
                                endDateTime=req_time,
                                durationStr='1 D',
                                barSizeSetting='1 min',
                                whatToShow='TRADES',
                                useRTH=False,
                                formatDate=1)
                            subctr += 1
                            print("Sub-counter: ",subctr)
                        print("Sub-counter: ", subctr)

                with custom_redirection(out):
                    print("Loaded {} new bars for contract: {}".format(len(bars),contract.localSymbol))
                print("Loaded {} new bars for contract: {}".format(len(bars), contract.localSymbol))

                if len(bars) != 0:
                    barsnewdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
                    for iy in range(0, len(bars)):
                        # print(bars[iy].dict())
                        barsnewdf.loc[iy] = [bars[iy].date.strftime("%m/%d/%Y %H:%M:%S"), bars[iy].open, bars[iy].high, bars[iy].low,
                                          bars[iy].close,
                                          bars[iy].volume, bars[iy].average, bars[iy].barCount]


                    if len(barsdf) != 0:
                        barsdf = pd.concat([barsnewdf, barsdf], ignore_index=True)
                        with custom_redirection(out):
                            print("Checking for duplicates: ",len(barsdf), len(barsdf.drop_duplicates()))
                    else:
                        barsdf = barsnewdf
                    bars = []


                new_length = barsdf.shape[0]

                print("--------------------OLDEST BAR----------------------------")
                print(barsdf.head(1).to_string())
                print("  ",newtime.strftime("%m/%d/%Y %H:%M:%S"))

                with custom_redirection(out):
                    print("--------------------OLDEST BAR----------------------------")
                    print(barsdf.head(1).to_string())
                    print("  ", newtime.strftime("%m/%d/%Y %H:%M:%S"))

                counter += 1
                #vol_index = min(int(16560), int(new_length))
                if new_length < 16560:
                    vol_index = new_length
                else:
                    vol_index = 16560

                print("Bars added: {} | Counter val: {} .  | Rolling Avg Volume: {} ".format(new_length-old_length, counter, barsdf.iloc[0:vol_index, 5].mean()))
                print("----------------------------------------------------------")
                with custom_redirection(out):
                    print("Bars added: {} | Counter val: {} .  | Rolling Avg Volume: {} ".format(
                        new_length - old_length, counter, barsdf.iloc[0:vol_index, 5].mean()))
                    print("----------------------------------------------------------")
                rolling_avg_volume = barsdf.iloc[0:vol_index, 5].mean()
                if rolling_avg_volume < 0.005:
                    if new_length < 100000:
                        rolling_avg_volume = 0.6969
                #print(pd.DataFrame.from_dict(vol_dict, orient='index').iloc[-1].to_string())
                print("******************************************************************************************************************")
                tyme.sleep(0.3)
            barsdf.to_csv(
                temp_dir + str(contract.localSymbol) + '_' + str(contract.lastTradeDateOrContractMonth) + '.csv')
        highest_counter += 1
    print("Terminating loop")

    with open(log_file_dir + contract.localSymbol + '.txt', 'a') as out:
        with custom_redirection(out):
            print("Terminating loop")

exit()







expiry='20231116'

file = file+'_'+expiry+'_'

for c in chains:
    if expiry in c.expirations:
        strikes = c.strikes
        tradingClass = c.tradingClass
        undconid = c.underlyingConId
        exch = c.exchange
        mult = c.multiplier

#es_price = 4332.50
es_price = 4513

strikes = [strike for strike in strikes if strike > 4350 and strike < 4650]

rights = ['C', 'P']
#strikes = [4180.0]

contracts_init = [Contract(symbol='ES', secType='FOP', multiplier = 50, lastTradeDateOrContractMonth=expiry, strike=strike, right=right, exchange='CME', currency='USD', tradingClass=tradingClass)
            for strike in strikes
            for right in rights]

contracts = []
for contr in contracts_init:
    if contr.right == 'P':
        if contr.strike < 45700:
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

