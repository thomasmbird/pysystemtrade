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
file = 'cl_contracts.csv'
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))


symbol_df = pd.DataFrame(columns=['Symbol','Sector'])
#symbol_df = pd.concat([symbol_df, pd.DataFrame({'Symbol': 'ES', 'Sector': 'USEquity'})],ignore_index=True)
symbol_df.loc['UB'] = ['UB', 'USEquity']
#symbol_df.loc['NQ'] = ['NQ', 'USEquity']
#symbol_df.loc['RTY'] = ['RTY', 'USEquity']
#symbol_df.loc['RS1'] = ['RS1', 'USEquity']
#symbol_df.loc['YM'] = ['YM', 'USEquity']
#symbol_df.loc['VX'] = ['VX', 'USEquity']

#symbol_df.loc['GC'] = ['GC', 'Metals']
#symbol_df.loc['SI'] = ['SI', 'Metals']
#symbol_df.loc['HG'] = ['HG', 'Metals']
#symbol_df.loc['PL'] = ['PL', 'Metals']

# symbol_df.loc['ZN'] = ['ZN', 'Rates']
# symbol_df.loc['ZF'] = ['ZF', 'Rates']
# symbol_df.loc['ZT'] = ['ZT', 'Rates']
# symbol_df.loc['UB'] = ['UB', 'Rates']
# symbol_df.loc['ZB'] = ['ZB', 'Rates']
# symbol_df.loc['TN'] = ['TN', 'Rates']
# symbol_df.loc['Z3N'] = ['Z3N', 'Rates']
# symbol_df.loc['TBF3'] = ['TBF3', 'Rates']
# symbol_df.loc['SOFR3'] = ['SOFR3', 'Rates']
# symbol_df.loc['SOFR1'] = ['SOFR1', 'Rates']#
# symbol_df.loc['ZQ'] = ['ZQ', 'Rates']

#symbol_df.loc['CL'] = ['CL', 'Energy']
#symbol_df.loc['NG'] = ['NG', 'Energy']
#symbol_df.loc['RB'] = ['RB', 'Energy']
#symbol_df.loc['HO'] = ['HO', 'Energy']

#symbol_df.loc['ZS'] = ['ZS', 'Grains']
#symbol_df.loc['ZL'] = ['ZL', 'Grains']
#symbol_df.loc['ZC'] = ['ZC', 'Grains']
#symbol_df.loc['ZW'] = ['ZW', 'Grains']
#symbol_df.loc['ZM'] = ['ZM', 'Grains']

#symbol_df.loc['KC'] = ['KC', 'Softs']
#symbol_df.loc['D'] = ['D', 'Softs']
#symbol_df.loc['SB'] = ['SB', 'Softs']
#symbol_df.loc['SF'] = ['SF', 'Softs']
#symbol_df.loc['W'] = ['W', 'Softs']
#symbol_df.loc['C'] = ['C', 'Softs']
#symbol_df.loc['CC'] = ['CC', 'Softs']

#symbol_df.loc['EUR'] = ['EUR', 'FX']
#symbol_df.loc['GBP'] = ['GBP', 'FX']
#symbol_df.loc['CHF'] = ['CHF', 'FX']
##symbol_df.loc['JPY'] = ['JPY', 'FX']
#symbol_df.loc['DX'] = ['DX', 'FX']
#symbol_df.loc['CAD'] = ['CAD', 'FX']
#symbol_df.loc['AUD'] = ['AUD', 'FX']
#symbol_df.loc['MXP'] = ['MXP', 'FX']

#symbol_df.loc['10Y'] = ['10Y', 'Micros']
#symbol_df.loc['2YY'] = ['2YY', 'Micros']
#symbol_df.loc['30Y'] = ['30Y', 'Micros']
#symbol_df.loc['MGC'] = ['MGC', 'Micros']
#symbol_df.loc['MHG'] = ['MHG', 'Micros']
#symbol_df.loc['MNQ'] = ['MNQ', 'Micros']
#symbol_df.loc['MRB'] = ['MRB', 'Micros']
#symbol_df.loc['YK']  = ['YK', 'Micros']
#symbol_df.loc['M2K'] = ['M2K', 'Micros']
#symbol_df.loc['MYM'] = ['MYM', 'Micros']
#symbol_df.loc['MES'] = ['MES', 'Micros']
#symbol_df.loc['MCL'] = ['MCL', 'Micros']
#symbol_df.loc['5YY'] = ['5YY', 'Micros']
#symbol_df.loc['MHO'] = ['MHO', 'Micros']
#symbol_df.loc['PLM'] = ['PLM', 'Micros']

#print(symbol_df.to_string())
#exit()

print('Total markets to analyze: ', symbol_df.shape[0])
# ES, NQ, RTY, RS1, YM
# VIX
#ZN, ZF, ZT, UB, ZB, TN, Z3N, TBF3
# GC, SI, HG, PL
# SOFR3, SOFR1, ZQ
# CL,NG, RB, HO,
# ZS, ZL, ZM, ZC, ZW
# 'KC, D, SB, SF, W, C, CC,
#Micros: 10Y, 2YY, 30Y, MGC, MHG, MNQ, MRB, YK, M2K, MYM, MES, MCL, 5YY
#matches = ib.reqMatchingSymbols('Euro')

for ic in range(0,symbol_df.shape[0]):
    try:
        sym = symbol_df['Symbol'].iloc[ic]
        sec = symbol_df['Sector'].iloc[ic]
        print('Starting Symbol: ',sym)

        if not os.path.exists(file_dir+sym+'/'):
            print('Creating ',file_dir+sym+'/')
            os.mkdir(file_dir+sym+'/')
            print('Creating ',file_dir+sym+'/'+'HistBars/')
            os.mkdir(file_dir+sym+'/'+'HistBars/')

        if not os.path.exists(file_dir+sym+'/'+'HistBars/'):
            print('Creating ', file_dir + sym + '/' + 'HistBars/')
            os.mkdir(file_dir+sym+'/'+'HistBars/')

        temp_dir = file_dir+sym+'/'+'HistBars/'

        contrtest = Contract(secType='FUT',symbol=sym,currency='USD',includeExpired=True)
        contrlist = ib.reqContractDetails(contract=contrtest)
        contrdf = util.df(contrlist)
        print(contrdf.to_string())

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

        iz = input("Input index for contract to pull historical data: ")
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
            req_time = contrexp + ' 15:55:00 US/Central'
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

                req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                print('Request starting at: {}'.format(req_time))

            else:
                barsdf = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
                print("No file to load, starting at present time")
                if past_expiry:
                    print("No file to load, past expiry so starting at: ", req_time)
                else:
                    req_time = ''

            counter = 0
            while counter < 23 and new_length != old_length and rolling_avg_volume > 0.005:
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

                    req_time = year + month + day + ' ' + hour + ':' + min + ':' + sec + ' US/Central'
                    print('Inner-loop Request starting at: {}'.format(req_time))

                if len(req_time) != 0:
                    #print("Next datapoint should be: ")
                    time = datetime.datetime.strptime(req_time.split(' ')[0] + ' ' + req_time.split(' ')[1],
                                                      "%Y%m%d %H:%M:%S")
                    if time.hour > 17 and time.hour <= 19:
                        print("Adding 1 hr for mkt close time")
                        newtime = time - datetime.timedelta(hours=5)
                    else:
                        newtime = time - datetime.timedelta(hours=4)
                        if newtime.hour == 15:
                            newtime = newtime - datetime.timedelta(hours=1)
                    #print("   ",newtime.strftime("%m/%d/%Y %H:%M:%S"))

                old_length = barsdf.shape[0]
                #print("Counter: ", counter)
                bars = []
                bars = ib.reqHistoricalData(
                    contrlist[iz],
                    endDateTime=req_time,
                    durationStr='14400 S',  # 120
                    barSizeSetting='5 secs',
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=1)
                print("Got past data call")
                if len(bars) == 0:
                    print("No bars!  Didn't work.  Time to loop further back in time.")
                    sec_start = 14400
                    subctr = 0
                    mult_fact = 1
                    while len(bars) == 0 and subctr < 10:
                        tyme.sleep(0.2)
                        if subctr == 5:
                            sec_start = 14400*2
                        if subctr == 6:
                            mult_fact = 2
                        if subctr == 7:
                            sec_start = 14400*4
                        if subctr == 8:
                            mult_fact = 4
                        #sec_start += 3600

                        time = datetime.datetime.strptime(req_time.split(' ')[0]+' '+req_time.split(' ')[1], "%Y%m%d %H:%M:%S")
                        print(time)
                        newtime = time - datetime.timedelta(hours=(4*mult_fact))
                        print(newtime)

                        new_req_time = datetime.datetime.strftime(newtime,"%Y%m%d %H:%M:%S")
                        req_time = new_req_time+' '+req_time.split(' ')[2]

                        print("Trying {} minus {} seconds".format(req_time, sec_start))
                        #print(str(sec_start)+' S')
                        bars = ib.reqHistoricalData(
                            contrlist[iz],
                            endDateTime=req_time,
                            durationStr=str(sec_start)+' S',
                            barSizeSetting='5 secs',
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