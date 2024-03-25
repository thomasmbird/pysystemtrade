import pandas as pd
import numpy as np
import seaborn as sns
import os.path
import pickle
import sklearn as sk
import plotly.express as px
import plotly.figure_factory as ff
import warnings
import itertools
import plotly.graph_objects as go
import plotly.graph_objs
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import matplotlib.pyplot as plt
from tqdm import tqdm
import plotly.colors
from PIL import ImageColor
import arctic
import datetime
from arctic import Arctic
from arctic.chunkstore.date_chunker import DateChunker
from arctic import CHUNK_STORE
import time as tyme
import datetime
import plotly.io as pio

store = Arctic('localhost')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/'

libnew = store['live_IBKR_data']
print(libnew.list_symbols())

#dr = pd.date_range("2023-11-02 22:00:00", "2023-11-5 00:00:00", freq="D")
dr = pd.date_range("2023-11-05 22:00:00", "2023-11-10 00:00:00", freq="D")

tt = tyme.time()
#es_df = libnew.read('TN   DEC 23_5sec_bars', columns=None, date_range=dr)

es_df = libnew.read('ESZ3_5sec_bars', columns=None, date_range=dr)
print("Took {} to load full data from db".format(tyme.time()-tt))
es_df.index = es_df.index.shift(-6, freq='H')
es_df = es_df.sort_index()
print("Start: {} to End: {}".format(es_df.index[0], es_df.index[-1]))
print("Total bars: {}".format(len(es_df)))
#print(es_df.head(20).to_string())
downsampled_df = es_df.resample('1min').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'count': 'sum', 'volume': 'sum'})
def get_open_price(input_df,date):
    if date:
        trading_day = date
    else:
        trading_day = pd.Timestamp.now() + pd.Timedelta(hours=6)
        trading_day = trading_day

    input_df = input_df.shift(6, freq='H')
    just_today = input_df.loc[trading_day.strftime('%m/%d/%Y')].dropna()
    #print(len(just_today))
    #print(just_today.head(5).to_string())
    #print(just_today.tail(5).to_string())

    return just_today['open'].iloc[0]

openprice = get_open_price(downsampled_df,None)
print("Open price was: {}".format(openprice))
print("Current Time is: {}".format(datetime.datetime.now()))
print("Final bar was: ")
print(downsampled_df.tail(2).to_string())

es_df = es_df.shift(6, freq='H')
print("Last data streamed was: ")
print(es_df.index[-1])
last_sec_data = es_df.index[-1]

stop_flag = False
while not stop_flag:
    print("Current time is: {}".format(datetime.datetime.now()))
    newest_data = libnew.max_date('ESZ3_5sec_bars')
    print("Newest data in db is: {}".format(newest_data))

    #if newest_data > last_sec_data:
    #    print("We have new data omgggg")
    if newest_data in es_df.index:
        print("We already have that data")
    else:
        print("New bar at {}".format(newest_data))
        tyme.sleep(5)
        rng = pd.date_range(start=newest_data.replace(tzinfo=None)-datetime.timedelta(seconds=5),end="2023-11-10 00:00:00",freq="D")
        try:
            new_es_df = libnew.read('ESZ3_5sec_bars',columns=None,date_range=rng)
            if len(new_es_df) > 0:
                print(new_es_df.shape)
                print(new_es_df.to_string())
        except Exception as e:
            print(e)

        try:
            new_es_df = libnew.read('ESZ3_5sec_bars',columns=None,date_range=pd.date_range("2023-11-05 22:00:00", "2023-11-10 00:00:00", freq="D"))
            print(new_es_df.index[-1])
        except Exception as e:
            print(e)


    #final_date = es_df.index[-1]
    #print("Final bar of first raw df: {}".format(final_date))
    #print("Downsample df final bar: {}".format(downsampled_df.index[-1]))

    #es_df = libnew.read('ESZ3_5sec_bars', columns=None, date_range=dr)


    tyme.sleep(5)