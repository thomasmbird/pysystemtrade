import datetime
from ib_insync import *
import pandas as pd
import csv
import os
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

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=52)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
file = 'ES_contracts.csv'

output_file = file_dir+file
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))

#sub = ScannerSubscription(
#    instrument='ESZ3',
#    locationCode='CME',
#    code =
#)

symbol_df = pd.DataFrame(columns=['Symbol','Sector'])
#symbol_df = pd.concat([symbol_df, pd.DataFrame({'Symbol': 'ES', 'Sector': 'USEquity'})],ignore_index=True)
symbol_df.loc['ES'] = ['ES', 'USEquity']
#symbol_df.loc['NQ'] = ['NQ', 'USEquity']
#symbol_df.loc['RTY'] = ['RTY', 'USEquity']
#symbol_df.loc['RS1'] = ['RS1', 'USEquity']
#symbol_df.loc['YM'] = ['YM', 'USEquity']

#symbol_df.loc['VIX'] = ['VIX', 'USEquity']
# symbol_df.loc['GC'] = ['GC', 'Metals']
# symbol_df.loc['SI'] = ['SI', 'Metals']
# symbol_df.loc['HG'] = ['HG', 'Metals']
# symbol_df.loc['PL'] = ['PL', 'Metals']

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

# symbol_df.loc['CL'] = ['CL', 'Energy']
# symbol_df.loc['NG'] = ['NG', 'Energy']
# symbol_df.loc['RB'] = ['RB', 'Energy']
# symbol_df.loc['HO'] = ['HO', 'Energy']

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
#
# symbol_df.loc['EUR'] = ['EUR', 'FX']
# symbol_df.loc['GBP'] = ['GBP', 'FX']
# #symbol_df.loc['CHF'] = ['CHF', 'FX']
# symbol_df.loc['JPY'] = ['JPY', 'FX']
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

print('Total markets to analyze: ',symbol_df.shape[0])
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
outdf = pd.DataFrame()
ctr = 0
for ic in range(0,symbol_df.shape[0]):

    sym = symbol_df['Symbol'].iloc[ic]
    sec = symbol_df['Sector'].iloc[ic]
    print('Starting Symbol: ',sym)

#        if not os.path.exists(file_dir+sym+'/'):
#            print('Creating ',file_dir+sym+'/')
#            os.mkdir(file_dir+sym+'/')
#            print('Creating ',file_dir+sym+'/'+'Hist1HrBars/')
#            os.mkdir(file_dir+sym+'/'+'Hist1HrBars/')

#        if not os.path.exists(file_dir+sym+'/'+'Hist1HrBars/'):
#            print('Creating ', file_dir + sym + '/' + 'Hist1HrBars/')
#            os.mkdir(file_dir+sym+'/'+'Hist1HrBars/')

#        temp_dir = file_dir+sym+'/'+'Hist1HrBars/'

    contrtest = Contract(secType='FUT',symbol=sym,currency='USD',includeExpired=True)
    contrlist = ib.reqContractDetails(contract=contrtest)
    contrdf = util.df(contrlist)

#    print(contrdf.to_string())

    contract_df = pd.DataFrame(columns=['secType','conID','symbol','localSymbol','exchange','multiplier','lastTradeDateOrContractMonth','AvgVolume'])

    contrlist = []

    for ix in range(0,contrdf.shape[0]):
        if 'QBALG' not in contrdf.iloc[ix][0].exchange:
            contrlist.append(contrdf.iloc[ix][0])

    print('Total contracts for this symbol: ', len(contrlist))

    contrlist.sort(key=myFunc)
    #contrlist = contrlist[0:10]
    print(contrlist)

    #termstructure_volume = pd.read_csv(file_dir + sym + '/term_structure_volume.csv')
    #print(termstructure_volume.to_string())

    for contr in contrlist:
        tempdict = contr.dict()
        print(list(tempdict.keys()))
        print([tempdict[key] for key in list(tempdict.keys())])
        cond = input("Type YES to add to rates contract list")

        if cond == "YES":
            tempdict = contr.dict()
            if outdf.empty:
                #print(list(tempdict.keys()))
                outdf = pd.DataFrame(columns=list(tempdict.keys()))

            outdf.loc[ctr] = [tempdict[key] for key in list(tempdict.keys())]
            #print(outdf.to_string())
            #print(outdf.dtypes)
            ctr += 1

            outdf.to_csv(file_dir+'es_contracts_temp.csv')
        else:
            pass



        #print(contrlist)



        #outdf = pd.DataFrame(contrlist.dict())
outdf.to_csv(output_file)