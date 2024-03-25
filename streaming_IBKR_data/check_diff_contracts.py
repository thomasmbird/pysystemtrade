import datetime
from ib_insync import *
import pandas as pd
import csv
from pathlib import Path

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=3)

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

contract_list = []

cle = Future('CL')
cds = ib.reqContractDetails(cle)
contract_df = util.df(cds)
#newcds = [cd in cds if cds.contract.exchange=='NYMEX']
newcds = []
print(newcds)

def myFunc(e):
    return e.contract.lastTradeDateOrContractMonth
#print(contract_df.loc[contract_df['Exchange'] == 'NYMEX'].to_String())
#print(contract_df.to_string())
for cd in cds:
    print(cd.contract)
    if cd.contract.exchange == 'NYMEX':
        newcds.append(cd)
        #print(cd)
newcds.sort(key=myFunc)

my_contracts = newcds[0:14]
header = list(my_contracts[0].contract.dict().keys())


outpath = Path(file_dir+file)

with outpath.open('w', newline="") as outfile:
    writer = csv.DictWriter(outfile,header)
    writer.writeheader()


    for contr in my_contracts:
        print("Writing to file")
        print(contr.contract.dict())
        writer.writerow(contr.contract.dict())
#print(util.df(my_contracts).to_string())




ib.disconnect()
