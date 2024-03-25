import datetime
from ib_insync import *
import pandas as pd
import csv
import os

def get_file_name(file_dir,file_name_base):
    today = datetime.datetime.today().strftime('%m_%d_%y')
    tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')

    if datetime.datetime.today().hour > 16:
        log_date = tomorrow
    else:
        log_date = today
    print('Choosing log date: ',log_date)
    file = file_dir+file_name_base+log_date+'.csv'
    if os.path.exists(file):
        print('{} already exists.'.format(file))
        file_mode = "append"
    else:
        print('{} will be created.'.format(file))
        file_mode="create"
    return file, file_mode


ib = IB()
ib.connect('127.0.0.1', 4001, clientId=6)

file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = 'booklevels_'

file_name, file_mode = get_file_name(file_dir, file_name_base)


contract = Contract(secType='FUT', conId=495512552, symbol='ES', lastTradeDateOrContractMonth='20231215', multiplier='50', exchange='CME', currency='USD', localSymbol='ESZ3', tradingClass='ES')
ib.qualifyContracts(contract)

req_fields = ['timestamp','level','bidSize','bidPrice','askPrice','askSize']

if file_mode == "create":
    if not os.path.exists(file_name):
        with open(file_name, "w", newline="") as filetowrite:
            print("Creating {}".format(file_name))
            writer = csv.writer(filetowrite, delimiter=',')
            writer.writerow(req_fields)

with open(file_name, "a", newline="") as filetowrite:
    stop_trigger = False

    while not stop_trigger:
        with open(file_name, "a", newline="") as filetowrite:
            writer = csv.writer(filetowrite, delimiter=',')
            try:
                ticker = ib.reqMktDepth(contract,numRows=5)
                ib.sleep(0.5)
                #print(ticker.domTicks)
                print('Time: ',ticker.domTicks[0].time)
                print('level, bidSize, bidPrice, askPrice, askSize')
                for i in range(0,5):
                    try:
                        print(ticker.domBids[i].size,ticker.domBids[i].price,ticker.domAsks[i].price,ticker.domAsks[i].size)
                    except Exception as e:
                        print(e)

                for i in range(0,5):
                    try:
                        writer.writerow([ticker.domTicks[i].time,str(i),ticker.domBids[i].size,ticker.domBids[i].price,ticker.domAsks[i].price,ticker.domAsks[i].size])
                    except Exception as e:
                        print(e)

                ib.cancelMktDepth(contract)
            except Exception as e:
                print(e)

        ib.sleep(5)
