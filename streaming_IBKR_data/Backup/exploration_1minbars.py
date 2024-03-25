import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
import numpy as np
import sys
from pathlib import Path

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

    def qual_contr(self):

        ib.qualifyContracts(self)
        print('Successfully qualified ',self.localSymbol)

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

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=55)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
file = '1minbar_contracts_metals.csv'
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))

symbol_df = pd.read_csv(file_dir+file)

#print(symbol_df.head(5))


print('Total markets to analyze: ', symbol_df.shape[0])


for ic in range(0,symbol_df.shape[0]):
    try:
        sym = symbol_df['Symbol'].iloc[ic]
        sec = symbol_df['Sector'].iloc[ic]
        print('Starting Symbol: ',sym)

        if not os.path.exists(file_dir+sym+'/'):
            print('Creating ',file_dir+sym+'/')
            os.mkdir(file_dir+sym+'/')
            print('Creating ',file_dir+sym+'/'+'Hist1mBars/')
            os.mkdir(file_dir+sym+'/'+'Hist1mBars/')

        if not os.path.exists(file_dir+sym+'/'+'Hist1mBars/'):
            print('Creating ', file_dir + sym + '/' + 'Hist1mBars/')
            os.mkdir(file_dir+sym+'/'+'Hist1mBars/')

        temp_dir = file_dir+sym+'/'+'Hist1mBars/'

        contrtest = Contract(secType='FUT',symbol=sym,currency='USD',includeExpired=True)
        contrlist = ib.reqContractDetails(contract=contrtest)
        contrdf = util.df(contrlist)
        #print(contrdf.to_string())

        contract_df = pd.DataFrame(columns=['secType','conID','symbol','localSymbol','exchange','multiplier','lastTradeDateOrContractMonth','AvgVolume'])

        contrlist = []

        for ix in range(0,contrdf.shape[0]):
            if 'QBALG' not in contrdf.iloc[ix][0].exchange:
                contrlist.append(contrdf.iloc[ix][0])

        print('Total contracts for this symbol: ', len(contrlist))
        #if len(contrlist) > 20:
        #    contrlist = contrlist[0:20]
        #    print('Shortening to just 20')

        contrlist.sort(key=myFunc)

        for i in range(0,len(contrlist)):
            print(i,contrlist[i])

        #iz = input("Input index for contract to pull historical data: ")
        for iz in range(0,9):
            iz = int(iz)
            #iz = 7
            print(contrlist[iz])
            contract = contrlist[iz]
            print(datetime.datetime.today())
            contrexp = contract.lastTradeDateOrContractMonth
            contr_expiry= datetime.datetime(year=int(contrexp[0:4]),month=int(contrexp[4:6]),day=int(contrexp[6::]))
            print(contr_expiry)
            if contr_expiry < datetime.datetime.today():
                print("Past Expiration!")
                past_expiry = True
                #req_time = str(contr_expiry.year) + str(contr_expiry.month) + str(contr_expiry.day) + ' 18:00:00 US/Central'
                req_time = contrexp + ' 16:55:00 US/Eastern'
                #req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                print(req_time)
            else:
                past_expiry = False

            ib.qualifyContracts(contrlist[iz])

            vol_dict = FixSizedDict(maxlen=5)

            #sys.stdout = open(temp_dir + str(contrlist[iz].localSymbol) + '_' + str(contrlist[iz].lastTradeDateOrContractMonth) + '.csv', 'w')



            new_length = 1
            old_length = 0
            rolling_avg_volume = 10
            while new_length > old_length and rolling_avg_volume > 0.005:

                log_file = temp_dir + str(contrlist[iz].localSymbol) + '_' + str(contrlist[iz].lastTradeDateOrContractMonth) + '.csv'
                #print("Starting Loop")
                if os.path.isfile(log_file):
                    barsdf = pd.read_csv(log_file, index_col=0)
                    #print(loaded_df.head(5).to_string())
                    start_time = barsdf['time'].iloc[0]
                    print("Loaded file with start_time: {}".format(start_time))


                    month = start_time.split('/')[0]
                    day = start_time.split('/')[1]
                    year = start_time.split('/')[2].split(' ')[0]
                    hour = start_time.split(' ')[1].split(':')[0]
                    # print("hour: ",hour)
                    min = start_time.split(' ')[1].split(':')[1]
                    sec = start_time.split(' ')[1].split(':')[2]

                    req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Eastern'
                    print('Request starting at: {}'.format(req_time))

                else:
                    barsdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
                    print("No file to load, starting at present time")
                    if past_expiry:
                        print("No file to load, past expiry so starting at: ", req_time)
                    else:
                        req_time = ''

                counter = 0
                while counter < 5 and new_length != old_length and rolling_avg_volume > 0.005:
                    if counter > 0:
                        start_time = str(barsdf['time'].iloc[0])
                        #print(start_time)
                        month = start_time.split('/')[0]
                        day = start_time.split('/')[1]
                        year = start_time.split('/')[2].split(' ')[0]
                        hour = start_time.split(' ')[1].split(':')[0]
                        # print("hour: ",hour)
                        min = start_time.split(' ')[1].split(':')[1]
                        sec = start_time.split(' ')[1].split(':')[2]

                        req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Eastern'
                        print('Inner-loop Request starting at: {}'.format(req_time))

                    if len(req_time) != 0:
                         #print("Next datapoint should be: ")
                         time = datetime.datetime.strptime(req_time.split(' ')[0] + ' ' + req_time.split(' ')[1],
                                                           "%Y%m%d %H:%M:%S")
                        #     if time.hour > 17 and time.hour <= 19:
                        #         print("Adding 1 hr for mkt close time")
                        #         newtime = time - datetime.timedelta(hours=5)
                        #     else:
                         newtime = time - datetime.timedelta(days=1)
                        #         if newtime.hour == 15:
                        #             newtime = newtime - datetime.timedelta(days=1)
                         print("   ",newtime.strftime("%m/%d/%Y %H:%M:%S"))

                    old_length = barsdf.shape[0]
                    #print("Counter: ", counter)
                    bars = []
                    bars = ib.reqHistoricalData(
                        contrlist[iz],
                        endDateTime=req_time,
                        durationStr='1 D',  # 120
                        barSizeSetting='1 min',
                        whatToShow='TRADES',
                        useRTH=False,
                        formatDate=1)
                    print("Got past data call")
                    if len(bars) == 0:
                        print("No bars!  Didn't work.  Time to loop further back in time.")

                        subctr = 0
                        mult_fact = 1
                        while len(bars) == 0 and subctr < 10:
                            tyme.sleep(0.2)

                            time = datetime.datetime.strptime(req_time.split(' ')[0]+' '+req_time.split(' ')[1], "%Y%m%d %H:%M:%S")
                            print(time)
                            newtime = time - datetime.timedelta(days=1)
                            print(newtime)

                            new_req_time = datetime.datetime.strftime(newtime,"%Y%m%d %H:%M:%S")
                            req_time = new_req_time+' '+req_time.split(' ')[2]

                            print("Trying {} minus 1 day".format(req_time))
                            #print(str(sec_start)+' S')
                            bars = ib.reqHistoricalData(
                                contrlist[iz],
                                endDateTime=req_time,
                                durationStr='1 D',
                                barSizeSetting='1 min',
                                whatToShow='TRADES',
                                useRTH=False,
                                formatDate=1)
                            subctr += 1
                            print("Sub-counter: ",subctr)

                    print("Loaded {} new bars".format(len(bars)))
                    if len(bars) != 0:
                        barsnewdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
                        for iy in range(0, len(bars)):
                            # print(bars[iy].dict())
                            barsnewdf.loc[iy] = [bars[iy].date.strftime("%m/%d/%Y %H:%M:%S"), bars[iy].open, bars[iy].high, bars[iy].low,
                                              bars[iy].close,
                                              bars[iy].volume, bars[iy].average, bars[iy].barCount]


                        if len(barsdf) != 0:
                            barsdf = pd.concat([barsnewdf, barsdf], ignore_index=True)
                            print(len(barsdf), len(barsdf.drop_duplicates()))
                        else:
                            barsdf = barsnewdf
                        bars = []


                    new_length = barsdf.shape[0]

                    #vol_dict[barsdf['time'].iloc[0]] = barsdf.iloc[0:17280, 5].mean()

                    print("--------------------OLDEST BAR----------------------------")
                    print(barsdf.head(1).to_string())
                    print("  ",newtime.strftime("%m/%d/%Y %H:%M:%S"))
                    #print("----------------------------------------------------------")
                    #print(new_length-old_length)
                    #print("----------------------------------------------------------")


                    counter += 1
                    #vol_index = min(int(16560), int(new_length))
                    if new_length < 16560:
                        vol_index = new_length
                    else:
                        vol_index = 16560

                    print("Bars added: {} | Counter val: {} .  | Rolling Avg Volume: {} ".format(new_length-old_length, counter, barsdf.iloc[0:vol_index, 5].mean()))
                    print("----------------------------------------------------------")
                    rolling_avg_volume = barsdf.iloc[0:vol_index, 5].mean()
                    if rolling_avg_volume < 0.005:
                        if new_length < 100000:
                            rolling_avg_volume = 0.6969
                    #print(pd.DataFrame.from_dict(vol_dict, orient='index').iloc[-1].to_string())
                    print("******************************************************************************************************************")
                    tyme.sleep(0.3)
                barsdf.to_csv(
                    temp_dir + str(contrlist[iz].localSymbol) + '_' + str(contrlist[iz].lastTradeDateOrContractMonth) + '.csv')

            print("Terminating loop")
    except Exception as e:
        print(e)