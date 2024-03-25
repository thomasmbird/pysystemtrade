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
ib.connect('127.0.0.1', 4001, clientId=8)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
file = 'allbars_2.csv'
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))


symbol_df = pd.DataFrame(columns=['Symbol','Sector'])
#symbol_df = pd.concat([symbol_df, pd.DataFrame({'Symbol': 'ES', 'Sector': 'USEquity'})],ignore_index=True)
# symbol_df.loc['ES'] = ['ES', 'USEquity']
# symbol_df.loc['NQ'] = ['NQ', 'USEquity']
# symbol_df.loc['RTY'] = ['RTY', 'USEquity']
# symbol_df.loc['RS1'] = ['RS1', 'USEquity']
# symbol_df.loc['YM'] = ['YM', 'USEquity']
#symbol_df.loc['VIX'] = ['VIX', 'USEquity']
#
# symbol_df.loc['GC'] = ['GC', 'Metals']
# symbol_df.loc['SI'] = ['SI', 'Metals']
# symbol_df.loc['HG'] = ['HG', 'Metals']
# symbol_df.loc['PL'] = ['PL', 'Metals']
# #
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
# # #
# symbol_df.loc['CL'] = ['CL', 'Energy']
# symbol_df.loc['NG'] = ['NG', 'Energy']
# symbol_df.loc['RB'] = ['RB', 'Energy']
# symbol_df.loc['HO'] = ['HO', 'Energy']
# # #
# symbol_df.loc['ZS'] = ['ZS', 'Grains']
# symbol_df.loc['ZL'] = ['ZL', 'Grains']
# symbol_df.loc['ZC'] = ['ZC', 'Grains']
# symbol_df.loc['ZW'] = ['ZW', 'Grains']
# symbol_df.loc['ZM'] = ['ZM', 'Grains']
#
# symbol_df.loc['KC'] = ['KC', 'Softs']
# symbol_df.loc['D'] = ['D', 'Softs']
# symbol_df.loc['SB'] = ['SB', 'Softs']
# symbol_df.loc['SF'] = ['SF', 'Softs']
# symbol_df.loc['W'] = ['W', 'Softs']
# symbol_df.loc['C'] = ['C', 'Softs']
# symbol_df.loc['CC'] = ['CC', 'Softs']
# # #
# symbol_df.loc['EUR'] = ['EUR', 'FX']
# symbol_df.loc['GBP'] = ['GBP', 'FX']
# symbol_df.loc['CHF'] = ['CHF', 'FX']
# symbol_df.loc['JPY'] = ['JPY', 'FX']
# symbol_df.loc['DX'] = ['DX', 'FX']
# symbol_df.loc['CAD'] = ['CAD', 'FX']
# symbol_df.loc['AUD'] = ['AUD', 'FX']
# symbol_df.loc['MXP'] = ['MXP', 'FX']
#
symbol_df.loc['10Y'] = ['10Y', 'Micros']
symbol_df.loc['2YY'] = ['2YY', 'Micros']
symbol_df.loc['5YY'] = ['5YY', 'Micros']
symbol_df.loc['30Y'] = ['30Y', 'Micros']
symbol_df.loc['MGC'] = ['MGC', 'Micros']
symbol_df.loc['MHG'] = ['MHG', 'Micros']
symbol_df.loc['MNQ'] = ['MNQ', 'Micros']
symbol_df.loc['MRB'] = ['MRB', 'Micros']
symbol_df.loc['YK'] = ['YK', 'Micros']
symbol_df.loc['M2K'] = ['M2K', 'Micros']
symbol_df.loc['MYM'] = ['MYM', 'Micros']
symbol_df.loc['MES'] = ['MES', 'Micros']
symbol_df.loc['MCL'] = ['MCL', 'Micros']
symbol_df.loc['5YY'] = ['5YY', 'Micros']
symbol_df.loc['MHO'] = ['MHO', 'Micros']
symbol_df.loc['PLM'] = ['PLM', 'Micros']

symbol_df.loc['MHNG'] = ['MHNG', 'Micros']
symbol_df.loc['MET'] = ['MET', 'Micros']
symbol_df.loc['MBT'] = ['MBT', 'Micros']
symbol_df.loc['MCO'] = ['MCO', 'Micros']
symbol_df.loc['SIL'] = ['SIL', 'Micros']
symbol_df.loc['M6A'] = ['M6A', 'Micros']
symbol_df.loc['M6B'] = ['M6B','Micros']
symbol_df.loc['MCD'] = ['MCD','Micros']
symbol_df.loc['MSF'] = ['MSF','Micros']
symbol_df.loc['M1B'] = ['M1B', 'Micros']
symbol_df.loc['J7'] = ['J7','Micros']
symbol_df.loc['E7'] = ['E7','Micros']
symbol_df.loc['YW'] = ['YW','Micros']
symbol_df.loc['YC'] = ['YC','Micros']
#symbol_df.loc['YK'] = ['YC','Micros']
#symbol_df.loc['YK'] = ['YK','Micros']

#symbol_df.loc['VX'] = ['VX', 'Equity']
#symbol_df.loc['VIX'] = ['VIX', 'Equity']
# symbol_df.loc['LE'] = ['LE','Livestock'] #HE/LN    #LE/48
# symbol_df.loc['HE'] = ['HE','Livestock']
# symbol_df.loc['BRE'] = ['BRE','FX']   #BR/6L
# symbol_df.loc['HH'] = ['HH','Energy']
# symbol_df.loc['NZD'] = ['NZD','FX']  #NE / 6N
# symbol_df.loc['GF'] = ['GF','Livestock'] #GF/62
# symbol_df.loc['EMD'] = ['EMD','Equity']    #EMD/ME
# symbol_df.loc['HP'] = ['HP','Energy']
# symbol_df.loc['QM'] = ['QM','Energy']   #E-mini crude
# symbol_df.loc['QG'] = ['QG','Energy']   #E-mini NG
# symbol_df.loc['BZ'] = ['BZ','Energy']

#
# #symbol_df.loc['KE'] = ['KE','Grains']    # EU Natgas
# symbol_df.loc['BB'] = ['BB','Energy']
# symbol_df.loc['DJUSRE'] = ['DJUSRE','Energy']
# symbol_df.loc['IXRE'] = ['IXRE','Energy']
# symbol_df.loc['LT'] = ['LT','Energy']
# symbol_df.loc['AC'] = ['AC','Energy']
# # symbol_df.loc['CSX'] = ['CSX','Energy']    #CSX / CS
#
# # symbol_df.loc['CPO'] = ['CPO','Energy']
# # symbol_df.loc['CU'] = ['CU','Energy']
# # symbol_df.loc['AUP'] = ['AUP','Metals']
#
# symbol_df.loc['DA'] = ['DA','Energy']   #DA/DC
# symbol_df.loc['PA'] = ['PA','Metals']
# symbol_df.loc['ZAR'] = ['ZAR','FX']
# # symbol_df.loc['HK'] = ['HK','Energy']    # HK / AHL
# # symbol_df.loc['RA'] = ['RA','FX'] #RA/6z
# # symbol_df.loc['RL'] = ['RL','Energy']   #RL/RLX
# symbol_df.loc['ZR'] = ['ZR','Energy']    #ZR / 14
# symbol_df.loc['Z3N'] = ['Z3N','Energy']   #Z3N / 3YR
# symbol_df.loc['CB'] = ['CB','Energy']
# symbol_df.loc['NF'] = ['NF','Energy']
# symbol_df.loc['CNH'] = ['CNH','Energy']
# symbol_df.loc['ETHUSDRR'] = ['ETHUSDRR','Energy']
# symbol_df.loc['BRR'] = ['BRR','Energy']
# symbol_df.loc['ALI'] = ['ALI','Energy']
# symbol_df.loc['ZO'] = ['ZO','Energy']
# symbol_df.loc['HRC'] = ['HRC','Energy']
# symbol_df.loc['LBR'] = ['LBR','Energy']
# symbol_df.loc['SEK'] = ['SEK','Energy']
# symbol_df.loc['SPXESUP'] = ['SPXESUP','Equity']
#ZO/O
# symbol_df.loc['S5M'] = ['S5M','Energy']
# # KW / KE WHEAT


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
output_file = file_dir+'allbars_micro.csv'
file = 'allbars_micro.csv'
total_contr_list = []
for ic in range(0,symbol_df.shape[0]):
    try:
        sym = symbol_df['Symbol'].iloc[ic]
        sec = symbol_df['Sector'].iloc[ic]
        print('Starting Symbol: ',sym)

        contrtest = Contract(secType='FUT',symbol=sym,currency='USD' )#,includeExpired=True)
        contrlist = ib.reqContractDetails(contract=contrtest)
        contrdf = util.df(contrlist)

        contract_df = pd.DataFrame(columns=['secType','conID','symbol','localSymbol','exchange','multiplier','lastTradeDateOrContractMonth','AvgVolume'])

        contrlist = []

        for ix in range(0,contrdf.shape[0]):
            if 'QBALG' not in contrdf.iloc[ix][0].exchange:
                contrlist.append(contrdf.iloc[ix][0])

        print('Total contracts for this symbol: ', len(contrlist))
        #contrlist = contrlist[0]
        #if len(contrlist) > 8:
        #    contrlist = contrlist[0:8]
        #    print('Shortening to just 8')

        contrlist.sort(key=myFunc)
        contrlist = contrlist[0:1]
        for i in range(0,len(contrlist)):
            print(i,contrlist[i])
            total_contr_list.append(contrlist[i])

    except Exception as e:
        print(e)



header = list(total_contr_list[0].dict().keys())

outpath = Path(file_dir + file)

with outpath.open('w', newline="") as outfile:
    writer = csv.DictWriter(outfile, header)
    writer.writeheader()

    for contr in total_contr_list:
        print("Writing to file")
        print(contr.dict())
        writer.writerow(contr.dict())


