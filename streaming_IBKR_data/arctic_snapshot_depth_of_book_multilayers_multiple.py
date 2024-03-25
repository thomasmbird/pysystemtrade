import datetime
from ib_insync import *
import pandas as pd
import csv
import os
from arctic import Arctic
from arctic import TICK_STORE
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

store = Arctic('localhost')
store.initialize_library('live_IBKR_data', lib_type=TICK_STORE)
lib = store['live_IBKR_data']

ib = IB()
ib.connect('127.0.0.1', 4001, clientId=63)

file_dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = 'booklevels_'

file_name, file_mode = get_file_name(file_dir, file_name_base)

contract = Contract(secType='FUT', symbol='ES', multiplier='50', exchange='CME', currency='USD', localSymbol='ESH4', tradingClass='ES')
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
                ticker = ib.reqMktDepth(contract, numRows=10)
                ib.sleep(0.5)
                #print(len(ticker))
                print(ticker)
                print(len(ticker.domTicks))
                #print(len(ticker.mktDepthData))
                print(ticker.domTicks)
                print('Time: ',ticker.domTicks[0].time)
                print('level, bidSize, bidPrice, askPrice, askSize')
                for i in range(0,10):
                    try:
                        print(ticker.domBids[i].size,ticker.domBids[i].price,ticker.domAsks[i].price,ticker.domAsks[i].size)
                    except Exception as e:
                        print(e)

                #for i in range(0,20):
                #    print(ticker.mktDepthData[i].)

                # for i in range(0,5):
                #     try:
                #         writer.writerow([ticker.domTicks[i].time,str(i),ticker.domBids[i].size,ticker.domBids[i].price,ticker.domAsks[i].price,ticker.domAsks[i].size])
                #     except Exception as e:
                #         print(e)
                # for i in range(0,5):
                #     try:
                #         lib.write('ESZ3_booklevels', [{'index': ticker.domTicks[i].time,
                #                                         'level' : i,
                #                                         'bidPrice': ticker.domBids[i].price,
                #                                         'bidSize': ticker.domBids[i].size,
                #                                         'askPrice': ticker.domAsks[i].price,
                #                                         'askSize': ticker.domAsks[i].size,
                #                                         }])
                #     except Exception as e:
                #         print(e)





                ib.cancelMktDepth(contract)
            except Exception as e:
                print(e)

        ib.sleep(5)
