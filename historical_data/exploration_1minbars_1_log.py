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
ib.connect('127.0.0.1', 4001, clientId=58)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
file = '1minbar_contracts_weird5.csv'
#start_ind = 174
log_file_dir = file_dir+'HistDataLogs/grains/'

def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))

symbol_df = pd.read_csv(file_dir+file)

#symbol_df = symbol_df.iloc[start_ind::]

print(symbol_df[['secType', 'symbol', 'lastTradeDateOrContractMonth', 'exchange', 'currency', 'localSymbol']].to_string())
start_ind = input("Choose new start_ind")
start_ind = int(start_ind)


symbol_df = symbol_df.iloc[start_ind::]
#with open(log_file, 'w') as out:
#    with custom_redirection(out):
   #     print('This text is redirected to file')
   #     print('So is this string')
   # print('This text is printed to stdout')

contract_object_list = []
for ix in range(0, symbol_df.shape[0]):
    contract_object_list.append(IBFutures(str(symbol_df['conId'].iloc[ix]),symbol_df['symbol'].iloc[ix],symbol_df['exchange'].iloc[ix],'FUT',
                                          symbol_df['lastTradeDateOrContractMonth'].iloc[ix],symbol_df['currency'].iloc[ix]))


print('Total contracts to analyze: ', len(contract_object_list))


ic = 0
for contract in contract_object_list[0::]:
    try:
        with open(log_file_dir+symbol_df['localSymbol'].iloc[ic]+'.txt', 'a') as out:
            local_sym = symbol_df['localSymbol'].iloc[ic]
            sym = symbol_df['symbol'].iloc[ic]
            print('Starting Symbol: ',local_sym)

            with custom_redirection(out):
                print("Attempting to qualify contract: ")
                print(ib.qualifyContracts(contract))
                if not os.path.exists(file_dir+sym+'/'):
                    print('Creating ',file_dir+sym+'/')
                    os.mkdir(file_dir+sym+'/')
                    print('Creating ',file_dir+sym+'/'+'Hist1mBars/')
                    os.mkdir(file_dir+sym+'/'+'Hist1mBars/')

                if not os.path.exists(file_dir+sym+'/'+'Hist1mBars/'):
                    print('Creating ', file_dir + sym + '/' + 'Hist1mBars/')
                    os.mkdir(file_dir+sym+'/'+'Hist1mBars/')

                temp_dir = file_dir+sym+'/'+'Hist1mBars/'
                print("Starting data pull at: ",datetime.datetime.today())


            contrexp = contract.lastTradeDateOrContractMonth
            contr_expiry= datetime.datetime(year=int(contrexp[0:4]),month=int(contrexp[4:6]),day=int(contrexp[6::]))
            with custom_redirection(out):
                print('contrexp: ',contrexp)
                print('contr_expiry: ',contr_expiry)

            if contr_expiry < datetime.datetime.today():
                with custom_redirection(out):
                    print("We are past this contracts expiration!")
                    past_expiry = True
                    #req_time = str(contr_expiry.year) + str(contr_expiry.month) + str(contr_expiry.day) + ' 18:00:00 US/Central'
                    #req_time = contrexp + ' 15:55:00 US/Central'
                    req_time = contrexp + ' 16:55:00 US/Eastern'
                    #req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                    print("Starting data pull at time: ",req_time)
            else:
                newtime = datetime.datetime.now() - datetime.timedelta(days=1)
                past_expiry = False

            vol_dict = FixSizedDict(maxlen=5)

            new_length = 1
            old_length = 0
            rolling_avg_volume = 10


        highest_counter = 0
        while new_length > old_length and rolling_avg_volume > 0.00005:

            print("Highest Lvl counter: ",highest_counter)
            with open(log_file_dir + symbol_df['localSymbol'].iloc[ic] + '.txt', 'a') as out:
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

                    #req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                    req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Eastern'
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

                counter = 0
                while counter < 5 and new_length != old_length and rolling_avg_volume > 0.00005:
                    print("Starting counter loop with val: ",counter)
                    with custom_redirection(out):
                        print("Starting counter loop with val: ", counter)
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
                        req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Eastern'
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
                    with custom_redirection(out):
                        bars = ib.reqHistoricalData(
                            contract,
                            endDateTime=req_time,
                            durationStr='1 D',  # 120
                            barSizeSetting='1 min',
                            whatToShow='TRADES',
                            useRTH=False,
                            formatDate=1)
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
        with open(log_file_dir + symbol_df['localSymbol'].iloc[ic] + '.txt', 'a') as out:
            with custom_redirection(out):
                print("Terminating loop")
    except Exception as e:
        print(e)
    ic+=1