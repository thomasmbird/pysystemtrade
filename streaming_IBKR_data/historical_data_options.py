import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import time as tyme
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
st = tyme.time()
ib = IB()
ib.connect('127.0.0.1', 4001, clientId=10)

#contract = Stock('TSLA', 'SMART', 'USD')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/'
file = 'cl_contracts.csv'
def onScanData(scanData):
    print(scanData[0])
    print(len(scanData))

#sub = ScannerSubscription(
#    instrument='ESZ3',
#    locationCode='CME',
#    code =
#)

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

sym = 'ES'

if not os.path.exists(file_dir + sym + '/Options/'):
    print('Creating ', file_dir + sym + '/Options/')
    os.mkdir(file_dir + sym + '/Options/')
    print('Creating ', file_dir + sym + '/Options/' + 'Hist1HrBars/')
    os.mkdir(file_dir + sym + '/' + 'Options/Hist1HrBars/')

if not os.path.exists(file_dir + sym + '/Options/' + 'Hist1HrBars/'):
    print('Creating ', file_dir + sym + '/Options/' + 'Hist1HrBars/')
    os.mkdir(file_dir + sym + '/Options/' + 'Hist1HrBars/')

temp_dir = file_dir + sym + '/Options/' + 'Hist1HrBars/'

und_contract = Contract(secType='FUT',symbol=sym,currency='USD',localSymbol='ESZ3',exchange='CME')
print(ib.qualifyContracts(und_contract))

chains = ib.reqSecDefOptParams(und_contract.symbol, und_contract.exchange, und_contract.secType, und_contract.conId)
print('Time elapsed: ',tyme.time()-st)
#print(util.df(chains).to_string())
#util.df(chains).to_csv(temp_dir+'chain_info.csv')
#exit()
contrs = []
for c in [chains[0]]:
    print(c)
    strikes = [strike for strike in c.strikes]
    # if strike % 5 == 0
    # and spxValue - 20 < strike < spxValue + 20]
    expirations = sorted(exp for exp in c.expirations)
    print("Strikes: ", len(strikes))
    print("Expirations: ", len(expirations))
    rights = ['P', 'C']
    # print(c.tradingClass)
    # print(expirations)
    # print(expirations[0])

    # exit()

    contracts = [
        Contract(symbol='ES', secType='FOP', lastTradeDateOrContractMonth=expiration, strike=strike, right=right,
                 currency='USD', exchange='CME', tradingClass=c.tradingClass)
        for right in rights
        for expiration in [expirations[0]]
        for strike in strikes]

    #contract = Contract(symbol='SPX',secType='OPT',lastTradeDateOrContractMonth='20231027', strike=4200, right='P',currency='USD',exchange='CBOE')
    contract = Contract(symbol='ES',secType='FOP',lastTradeDateOrContractMonth='20231027',exchange='CME')

    print(contract)
    #print(contracts[0])
    ib.qualifyContracts(contract)
    exit()


for c in chains[12::]:
    print('Starting ',c.tradingClass)
    if c.tradingClass == 'E5A' or c.tradingClass == 'ES':
        print('Skipped ',c.tradingClass)
        continue

    sc = tyme.time()
    #print(c)
    strikes = [strike for strike in c.strikes]
               #if strike % 5 == 0
               #and spxValue - 20 < strike < spxValue + 20]
    expirations = sorted(exp for exp in c.expirations)
    print("Strikes: ",len(strikes))
    print("Expirations: ",len(expirations))
    rights = ['P', 'C']
    #print(c.tradingClass)
    #print(expirations)
    #print(expirations[0])

    #exit()

    contracts = [Contract(symbol='ES',secType='FOP',lastTradeDateOrContractMonth=expiration,strike=strike,right=right,currency='USD',exchange='CME',tradingClass=c.tradingClass)
                 for right in rights
                 for expiration in [expirations[0]]
                 for strike in strikes]
    #contr = Contract(symbol='ES',secType='FOP',lastTradeDateOrContractMonth=expirations[0],strike=strikes[0],right=rights[0],currency='USD',exchange='CME')
    print('gothere')
    #print('Time elapsed: ', tyme.time() - st)
    #contracts = ib.qualifyContracts(*contracts)
    print('Time elapsed getting contracts: ', tyme.time() - st)
    st=tyme.time()

    #tickerlist = ib.reqTickers(*contracts,regulatorySnapshot=False)
    #print(util.df(tickerlist).to_string())
    #print('Time elapsed: ',tyme.time()-st)
    #util.df(tickerlist).to_csv(temp_dir+c.tradingClass+'.csv')
    #print(ib.qualifyContracts(contr))
    #df = pd.DataFrame(columns=['strike','kind','close','last','bid','ask','mid','volume'])
    df = pd.DataFrame()
    l = []
    for x in contracts:
        snapshot = ib.reqMktData(x,"", True, False)
        l.append([x.strike, x.right, snapshot])
    print('Time elapsed calling reqMktData: ', tyme.time() - st)
    st=tyme.time()
    while util.isNan(snapshot.bid):
        ib.sleep()
    ib.sleep(10)
    print('Time elapsed waiting for mkt data to fill in', tyme.time() - st)
    st=tyme.time()

    outdf = pd.DataFrame()

    counter = 0
    for ii in l:
        try:
            tempdict = {}
            #tempdict['Contract Code'] = ii[2].contract.tradingClass
            #tempdict['']
            #tempdict = ii[2].dict()
            ##tempdict['strike'] = ii[0]
            #tempdict['kind'] = ii[1]
            datadict = ii[2].dict()
            for item in datadict.keys():
                #print(item, datadict[item],type(datadict[item]))
                if item == 'time':
                    #print(datadict[item].strftime('%m/%d/%Y'))
                    #print(datadict[item].strftime('%H:%M:%S'))
                    tempdict['day'] = datadict[item].strftime('%m/%d/%Y')
                    tempdict['time'] = datadict[item].strftime('%H:%M:%S')
                if item == 'contract':
                    #print(datadict[item].strike)
                    tempdict['symbol'] = c.tradingClass
                    tempdict['expiration'] = datadict[item].lastTradeDateOrContractMonth
                    tempdict['strike'] = datadict[item].strike
                    tempdict['right'] = datadict[item].right
                elif item == 'bidGreeks' or item == 'askGreeks' or item == 'lastGreeks' or item == 'modelGreeks':
                    #print(datadict[item].impliedVol)
                    print(item,datadict[item])
                    if datadict[item] == None:
                        tempdict[item + '_tickAttrib'] = 0
                        tempdict[item + '_impliedVol'] = 0
                        tempdict[item + '_delta'] = 0
                        tempdict[item + '_optPrice'] = 0
                        tempdict[item + '_pvDividend'] = 0
                        tempdict[item + '_gamma'] = 0
                        tempdict[item + '_vega'] = 0
                        tempdict[item + '_theta'] = 0
                        tempdict[item + '_undPrice'] = 0
                    else:
                        tempdict[item+'_tickAttrib'] = datadict[item].tickAttrib
                        tempdict[item + '_impliedVol'] = datadict[item].impliedVol
                        tempdict[item + '_delta'] = datadict[item].delta
                        tempdict[item + '_optPrice'] = datadict[item].optPrice
                        tempdict[item + '_pvDividend'] = datadict[item].pvDividend
                        tempdict[item + '_gamma'] = datadict[item].gamma
                        tempdict[item + '_vega'] = datadict[item].vega
                        tempdict[item + '_theta'] = datadict[item].theta
                        tempdict[item + '_undPrice'] = datadict[item].undPrice
                elif item == 'ticks' or item == 'tickByTicks' or item == 'domBids' or item == 'domAsks' or item == 'domTicks':
                    #print('Not saving ',item)
                    #print(len(item))#
                    unused_var = 0.0
                    #try:
                        #print(datadict[item].dict())
                    #except:
                        #print(datadict[item])
                elif item != 'poop':
                    tempdict[item] = datadict[item]
            #print(tempdict)

            #print(pd.DataFrame(tempdict,index=[counter]).to_string())

                #if len(ii2.dict()[item] != 0):
                #    print("Item was a list, unpacking")
                #    for inner_item in ii2.dict()[item]:
                #        print(inner_item)
            if outdf.empty:
                print("Creating outdf")
                outdf = pd.DataFrame(tempdict,index=[counter])
            else:
                tempdf = pd.DataFrame(tempdict,index=[counter])
                outdf = pd.concat([outdf, tempdf], ignore_index=True)
            counter += 1
            print('Finished run number: ',counter,' with strike: ',datadict['contract'].strike)
        except Exception as e:
            print('Failed on ',datadict['contract'].strike)
            print(datadict)
            print(e)
            counter += 1
        #print(tempdict)
        #tempdf = pd.DataFrame(tempdict)
        #df = pd.concat([df, tempdf], ignore_index=True)
        #counter += 1


        #df = pd.concat([df, pd.DataFrame({'strike': ii[0], 'kind': ii[1], 'close': ii[2].close, 'last': ii[2].last, 'bid': ii[2].bid,
        #                'ask': ii[2].ask, 'mid': (ii[2].bid + ii[2].ask) / 2, 'volume': ii[2].volume},index=[0])],ignore_index=True)
    outdf.to_csv(temp_dir + c.tradingClass+ '_' + datadict['contract'].lastTradeDateOrContractMonth+'_.csv')

    print(df.head(5).to_string())

    print('Time elapsed doing chain ',c.tradingClass,': ',tyme.time() - sc)

#ib.reqMarketDataType(4)

#contrlist = ib.reqContractDetails(contract=und_contract)
#contrdf = util.df(contrlist)
#print(contrdf.to_string())


ib.disconnect()
exit()
for ic in range(0,symbol_df.shape[0]):
    try:
        sym = symbol_df['Symbol'].iloc[ic]
        sec = symbol_df['Sector'].iloc[ic]
        print('Starting Symbol: ',sym)

        if not os.path.exists(file_dir+sym+'/'):
            print('Creating ',file_dir+sym+'/')
            os.mkdir(file_dir+sym+'/')
            print('Creating ',file_dir+sym+'/'+'Hist1HrBars/')
            os.mkdir(file_dir+sym+'/'+'Hist1HrBars/')

        if not os.path.exists(file_dir+sym+'/'+'Hist1HrBars/'):
            print('Creating ', file_dir + sym + '/' + 'Hist1HrBars/')
            os.mkdir(file_dir+sym+'/'+'Hist1HrBars/')

        temp_dir = file_dir+sym+'/'+'Hist1HrBars/'

        contrtest = Contract(secType='FUT',symbol=sym,currency='USD')
        contrlist = ib.reqContractDetails(contract=contrtest)
        contrdf = util.df(contrlist)
        print(contrdf.head(5).to_string())

        contract_df = pd.DataFrame(columns=['secType','conID','symbol','localSymbol','exchange','multiplier','lastTradeDateOrContractMonth','AvgVolume'])

        contrlist = []

        for ix in range(0,contrdf.shape[0]):
            if 'QBALG' not in contrdf.iloc[ix][0].exchange:
                contrlist.append(contrdf.iloc[ix][0])

        print('Total contracts for this symbol: ', len(contrlist))
        if len(contrlist) > 20:
            contrlist = contrlist[0:20]
            print('Shortening to just 20')

        contrlist.sort(key=myFunc)

        for iz in range(0,len(contrlist)):
            #print(contrlist[iz])
            print(contrlist[iz].localSymbol,contrlist[iz].lastTradeDateOrContractMonth)
            ib.qualifyContracts(contrlist[iz])

            bars = ib.reqHistoricalData(
                    contrlist[iz],
                    endDateTime='',
                    durationStr='120 D',  #120
                    barSizeSetting='1 hour',
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=1)

            barsdf = pd.DataFrame(columns=['time','open','high','low','close','volume','average','barCount'])
            for iy in range(0,len(bars)):
                #print(bars[iy].dict())
                barsdf.loc[iy] = [bars[iy].date.strftime("%m/%d/%Y %H:%M:%S"),bars[iy].open,bars[iy].high,bars[iy].low,bars[iy].close,
                                  bars[iy].volume,bars[iy].average,bars[iy].barCount]

            #print(barsdf.to_string())
            #print(barsdf['volume'].mean())
            barsdf.to_csv(temp_dir+str(contrlist[iz].localSymbol)+'_'+str(contrlist[iz].lastTradeDateOrContractMonth)+'.csv')

            contract_df.loc[iz] = [contrlist[iz].secType,contrlist[iz].conId,contrlist[iz].symbol,contrlist[iz].localSymbol,contrlist[iz].exchange,
                                   contrlist[iz].multiplier,contrlist[iz].lastTradeDateOrContractMonth,barsdf['volume'].mean()]

            #print(contract_df.to_string())

            ib.sleep(1)
        contract_df = contract_df.fillna(0)
        #print(contract_df.to_string())
        contract_df.to_csv(file_dir+sym+'/term_structure_volume.csv')
    except Exception as e:
        print(e)

ib.disconnect()
#print(util.df(contrlist).to_string())

#contr = Contract(secType='FUT', conId=642483362, currency='USD', exchange='CME')
#ib.qualifyContracts(contr)
#print(contr)

#for ix in range(0,df.shape[0]):
    #print(df.iloc[ix][1])
    #if 'FUT' in df.iloc[ix][1]:
    #    print(df.iloc[ix][0])
    #if 'FUT' in df.index[ix]:
    #    print(df.iloc[ix])



#print(util.df(matches).to_string())

exit()
