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
from arctic import CHUNK_STORE, TICK_STORE
import time as tyme
import datetime
import plotly.io as pio
import warnings

def get_trading_day():
    today = datetime.datetime.today().strftime('%m_%d_%y')
    tomorrow = (datetime.datetime.today() + datetime.timedelta(days=1)).strftime('%m_%d_%y')
    if datetime.datetime.today().hour > 16:
        trading_date = tomorrow
    else:
        trading_date = today
    print('Choosing trading date: ',trading_date)
    return trading_date

def unflatten_trades(input_df):
    loaded_trades_list = []
    for ix in range(0,len(input_df.index)):
        newtrade = {}
        newtrade['entry_rules'] = {}
        newtrade['exit_rules'] = {}
        for column in input_df.columns:
            if 'entry_rules' in column:
                #print(column.split('_'))
                newtrade['entry_rules'][column.replace('entry_rules_','')] = input_df[column].iloc[ix]
            elif 'exit_rules' in column:
                newtrade['exit_rules'][column.replace('exit_rules_','')] = input_df[column].iloc[ix]
            else:
                newtrade[column] = input_df[column].iloc[ix]

        if newtrade['active'] == 0:
            newtrade['active'] = False
        elif newtrade['active'] == 1:
            newtrade['active'] = True
        else:
            print("Active value didn't make sense")
        newtrade['entry_time'] = datetime.datetime.strptime(newtrade['entry_time'], '%m-%d-%Y %H:%M:%S')
        loaded_trades_list.append(newtrade)
    return loaded_trades_list

trading_date = get_trading_day()

warnings.filterwarnings('ignore')

store = Arctic('localhost')
file_dir = '/Users/thomasmbird/Documents/Quant Stuff/'
buysell_dict = {'buy': 1.0, 'sell': -1.0}
#store.initialize_library('second_try_live_trading', lib_type=TICK_STORE)
marketdatalib = store['live_IBKR_data']
tradinglib = store['second_try_live_trading']

todays_live_trades = []


if 'trades_{}'.format(trading_date) in tradinglib.list_symbols():
    print("****************** There are trades in Today's DB!!! *******************")
    trades_in_db = tradinglib.read('trades_{}'.format(trading_date))
    print(trades_in_db.to_string())
    input_val = input("Type YES to load.")
    if input_val == "YES":
        todays_live_trades = unflatten_trades(trades_in_db)
        for trade in todays_live_trades:
            print("Loaded trade {} with values:".format(trade['trade_name']))
            print(pd.DataFrame(trade).to_string())
            #

if len(todays_live_trades) == 0:
    all_stops = 0.8
    all_start_time = "00:08"

    todays_live_trades = [{'trade_name': "ES_Reversion",
    'trade_market': 'ESZ3',
    'side': 'buy',
    'entry_rules': {
        'move_from_open': -1,
        'move_from_ma': -1,
        'ma_window': 30,
        'time_start': all_start_time},
    'exit_rules': {
        'stop_loss': all_stops,
        'exit_on_close': 1},

    'size': 20.95,
    'active': False,
    'multiplier': 50},

    {
    'trade_name': "CrudeOil_Momentum",
    'trade_market': "CLZ3",
    'side': 'buy',
    'entry_rules': {
        'move_from_open': -1,
        'move_from_ma': -1,
        'ma_window': 30,
        'time_start': all_start_time},
    'exit_rules': {
        'stop_loss': all_stops,
        'exit_on_close': 1},
    'size': 21.5,
    'active': False,
    'multiplier': 1000},
    {
    'trade_name': "TenYearNote_Momentum_1",
    'trade_market': "TN   DEC 23",
    'side': 'sell',
    'entry_rules': {
      'move_from_open': -1,
      'move_from_ma': -1,
      'ma_window': 30,
      'time_start': all_start_time},
    'exit_rules': {
      'stop_loss': all_stops,
      'exit_on_close': 1},
    'size': 21.5,
    'active': False,
    'multiplier': 1000}
                      ]

daily_params = {}
daily_params['ESZ3'] = {}
daily_params['CLZ3'] = {}
daily_params["TN   DEC 23"] = {}

daily_params['ESZ3']['10DATR'] = 60.15
daily_params['CLZ3']['10DATR'] = 2.93
daily_params["TN   DEC 23"]['10DATR'] = 1.2
size_conversion_dict = {}
size_conversion_dict['ESZ3'] = 1
size_conversion_dict['NQZ3'] = 1
size_conversion_dict['CLZ3'] = 0.9743
size_conversion_dict["TN   DEC 23"] = 1

def on_bar_update(downsampled_df, market, daily_params, trade):
    #output_tradelist = []
    print("CHECKING INDICATORS FOR {} TRADE ENTRY".format(market))
    df_with_indicators = downsampled_df.copy(deep=True)

    df_with_indicators['move_from_open'] = df_with_indicators['close'] - daily_params['daily_open']
    df_with_indicators['move_from_open'] = df_with_indicators['move_from_open'] / daily_params['10DATR']

    df_with_indicators['move_from_ma'] = df_with_indicators['close'] - df_with_indicators['close'].rolling(30, min_periods=1).mean()
    df_with_indicators['move_from_ma'] = df_with_indicators['move_from_ma'] / daily_params['10DATR']

    print('Value for {} move_from_open: '.format(market), df_with_indicators['move_from_open'].iloc[-1])
    print('Value for {} move_from_ma: '.format(market), df_with_indicators['move_from_ma'].iloc[-1])
    print('Value for {} time_entry: '.format(market), df_with_indicators.index[-1].hour,df_with_indicators.index[-1].minute)

    #tstamp = pd.Timestamp(hour=1, minute=3,year=df_with_indicators.index[-1].year,month=df_with_indicators.index[-1].month,day=df_with_indicators.index[-1].day,
    #      tz=df_with_indicators.index[-1].tz)

    if not trade['active']:
        entry_array = []
        enter_trade = 0

        if trade['side'] == 'buy':
            if 'time_start' in trade['entry_rules']:
                tstamp = pd.Timestamp(hour=int(trade['entry_rules']['time_start'].split(":")[0]), minute=int(trade['entry_rules']['time_start'].split(":")[1]),
                                      year=df_with_indicators.index[-1].year,
                                      month=df_with_indicators.index[-1].month,
                                      day=df_with_indicators.index[-1].day,
                                      tz=df_with_indicators.index[-1].tz)
                if df_with_indicators.index[-1] > tstamp:
                    print("We are beyond {} time entry threshold".format(market))
                    print(tstamp)
                else:
                    entry_array.append(False)

            if 'move_from_open' in trade['entry_rules']:
                if df_with_indicators['move_from_open'].iloc[-1] > trade['entry_rules']['move_from_open']:
                    print("Above {} threshold for move from open!".format(market))
                else:
                    entry_array.append(False)
            if 'move_from_ma' in trade['entry_rules']:
                if df_with_indicators['move_from_ma'].iloc[-1] > trade['entry_rules']['move_from_ma']:
                    print("Above {} threshold for move from ma!".format(market))
                else:
                    entry_array.append(False)

            if len(entry_array) == 0:
                print("We are above all {} thresholds for buy, entering trade: ".format(market), trade['trade_name'])
                trade['entry_price'] = df_with_indicators['close'].iloc[-1]+0.25
                trade['active'] = True
                if 'stop_loss' in trade['exit_rules']:
                    trade['stop_price'] = trade['entry_price'] - trade['exit_rules']['stop_loss']*daily_params['10DATR']
                    print("Stop loss for {} at: {}".format(market,trade['stop_price']))

                trade['entry_time'] = df_with_indicators.index[-1]
                new_trade_metadata = flatten_trade_dict(trade)
                new_trade_metadata['index'] = df_with_indicators.index[-1]
                new_trade_metadata['entry_time'] = new_trade_metadata['entry_time'].strftime('%m-%d-%Y %H:%M:%S')
                new_trade_metadata['type'] = 'entry'
                tradinglib.write('trades_{}'.format(trading_date), [new_trade_metadata])

            else:
                print("Did not meet at least one {} threshold, no trade entry".format(market))

        elif trade['side'] == 'sell':
            if 'time_start' in trade['entry_rules']:
                tstamp = pd.Timestamp(hour=int(trade['entry_rules']['time_start'].split(":")[0]), minute=int(trade['entry_rules']['time_start'].split(":")[1]),
                                      year=df_with_indicators.index[-1].year,
                                      month=df_with_indicators.index[-1].month,
                                      day=df_with_indicators.index[-1].day,
                                      tz=df_with_indicators.index[-1].tz)
                if df_with_indicators.index[-1] > tstamp:
                    print("We are beyond {} time entry threshold".format(market))
                    print(tstamp)
                else:
                    entry_array.append(False)
            if 'move_from_open' in trade['entry_rules']:
                if df_with_indicators['move_from_open'].iloc[-1] < -trade['entry_rules']['move_from_open']:
                    print("Below {} threshold for move from open!".format(market))
                else:
                    entry_array.append(False)
            if 'move_from_ma' in trade['entry_rules']:
                if df_with_indicators['move_from_ma'].iloc[-1] < -trade['entry_rules']['move_from_ma']:
                    print("Below {} threshold for move from ma!".format(market))
                else:
                    entry_array.append(False)

            if len(entry_array) == 0:
                print("We are below all {} thresholds for sell, entering trade: ".format(market), trade['trade_name'])
                trade['entry_price'] = df_with_indicators['close'].iloc[-1] - 0.25
                if 'stop_loss' in trade['exit_rules']:
                    trade['stop_price'] = trade['entry_price'] + trade['exit_rules']['stop_loss']*daily_params['10DATR']
                    print("Stop loss for {} at: {}".format(market,trade['stop_price']))
                trade['active'] = True
                trade['entry_time'] = df_with_indicators.index[-1]
                new_trade_metadata = flatten_trade_dict(trade)
                new_trade_metadata['index'] = df_with_indicators.index[-1]
                new_trade_metadata['entry_time'] = new_trade_metadata['entry_time'].strftime('%m-%d-%Y %H:%M:%S')
                new_trade_metadata['type'] = 'entry'
                tradinglib.write('trades_{}'.format(trading_date), [new_trade_metadata])

                #tradinglib.write('trades_{}'.format(trading_date),[new_trade_metadata])
            else:
                print("Did not meet at least one {} threshold, no trade entry".format(market))

        else:
            print('ERROR!!!!! Trade {} had no buy/sell side!!!!'.format(trade['trade_name']))

    elif trade['active']:
        print(trade['trade_name']+"is on, checking exit rules.")
        if trade['side'] == 'buy':
            pnl = buysell_dict[trade['side']]*trade['size']*50*(df_with_indicators['close'].iloc[-1] - trade['entry_price'])
            if 'stop_price' in trade.keys():
                print("Checking for {} stop loss".format(market))
                if df_with_indicators['close'].iloc[-1] < trade['stop_price']:
                    print("Exiting trade {} via stop loss at: {}".format(trade['trade_name'],df_with_indicators['close'].iloc[-1]))
                    print("Final {} PnL: {}".format(trade['trade_name'],pnl))
                    trade['exit_price'] = df_with_indicators['close'].iloc[-1]
                    trade['active'] = False
                    trade['exit_time'] = df_with_indicators.index[-1]

                    new_trade_metadata = flatten_trade_dict(trade)
                    new_trade_metadata['index'] = df_with_indicators.index[-1]
                    #if type(new_trade_metadata['entry_time']) != type(str):
                    new_trade_metadata['entry_time'] = new_trade_metadata['entry_time'].strftime(
                            '%m-%d-%Y %H:%M:%S')
                    #if type(new_trade_metadata['exit_time']) != type(str):
                    new_trade_metadata['exit_time'] = new_trade_metadata['exit_time'].strftime('%m-%d-%Y %H:%M:%S')
                    new_trade_metadata['type'] = 'exit'
                    tradinglib.write('trades_{}'.format(trading_date), [new_trade_metadata])

                else:
                    print("Current {} price is {} vs a stop price of {}".format(market, df_with_indicators['close'].iloc[-1],trade['stop_price']))

        elif trade['side'] == 'sell':

            pnl = buysell_dict[trade['side']]*trade['size']*50*(df_with_indicators['close'].iloc[-1] - trade['entry_price'])
            if 'stop_price' in trade.keys():
                print("Checking {} for stop loss".format(market))
            if df_with_indicators['close'].iloc[-1] > trade['stop_price']:
                print("Exiting trade {} via stop loss at: {}".format(trade['trade_name'],df_with_indicators['close'].iloc[-1]))
                print("Final {} PnL: {}".format(trade['trade_name'], pnl))
                trade['exit_price'] = df_with_indicators['close'].iloc[-1]
                trade['active'] = False
                trade['exit_time'] = df_with_indicators.index[-1]

                new_trade_metadata = flatten_trade_dict(trade)
                new_trade_metadata['index'] = df_with_indicators.index[-1]
                #if type(new_trade_metadata['entry_time']) != type(str):
                new_trade_metadata['entry_time'] = new_trade_metadata['entry_time'].strftime('%m-%d-%Y %H:%M:%S')
                #if type(new_trade_metadata['exit_time']) != type(str):
                new_trade_metadata['exit_time'] = new_trade_metadata['exit_time'].strftime('%m-%d-%Y %H:%M:%S')
                new_trade_metadata['type'] = 'exit'
                tradinglib.write('trades_{}'.format(trading_date), [new_trade_metadata])


            else:
                print("Current {} price is {} vs a stop price of {}".format(market, df_with_indicators['close'].iloc[-1],
                                                                         trade['stop_price']))

        else:
            print('ERROR!!!!! Trade {} had no buy/sell side!!!!'.format(trade['trade_name']))


    else:
        print("ERROR!!!!")
        print("{} active setting was neither True nor False".format(trade['trade_name']))

    return trade
def check_for_live_pnl(price, trade):
    total_pnl = 0
    total_risk = 0
    total_pnl += buysell_dict[trade['side']]*trade['size']*trade['multiplier']*(price - trade['entry_price'])
    total_risk += buysell_dict[trade['side']]*trade['size']*trade['multiplier']*(trade['stop_price']-price)
    #print("Total {} PnL: {}".format(market, total_pnl))
    return total_pnl, total_risk
def get_open_price(input_df, date):
    if date:
        trading_day = date
    else:
        trading_day = pd.Timestamp.now() + pd.Timedelta(hours=7)
        trading_day = trading_day

    #input_df = input_df.shift(6, freq='H')
    just_today = input_df.loc[trading_day.strftime('%m/%d/%Y')].dropna()
    print("Open price was {} on {}".format(just_today['open'].iloc[0], trading_day.strftime('%m/%d/%Y')))
    #print(len(just_today))
    #print(just_today.head(5).to_string())
    #print(just_today.tail(5).to_string())

    return just_today['open'].iloc[0]

def flatten_trade_dict(tradedict):
    new_dict = {}
    for item in tradedict.keys():
        if item == 'active':
            if tradedict[item]:
                new_dict[item] = 1
            else:
                new_dict[item] = 0

        elif type(tradedict[item]) == type(dict()):
            for subitem in tradedict[item]:
                new_dict[item+'_'+subitem] = tradedict[item][subitem]

        else:
            new_dict[item] = tradedict[item]

    return new_dict
#indicator_list = []
# indicator_list.append({'name': 'move_from_open',
#              'baseline': 'daily_open',
#              'comparison_value': 'close',
#              'normalize_by' : '10DATR'
# })
# indicator_list.append({'name': 'move_from_ma',
#              'baseline': 'ma',
#              'ma_windodw': 30,
#              'comparison_value': 'close',
#              'normalize_by' : '10DATR'
# })
def initialize_indicators(downsampled_df, daily_params):
    df_with_indicators = downsampled_df

    df_with_indicators['move_from_open'] = df_with_indicators['close'] - daily_params['daily_open']
    df_with_indicators['move_from_open'] = df_with_indicators['move_from_open'] / daily_params['10DATR']

    df_with_indicators['move_from_ma'] = df_with_indicators['close'] - df_with_indicators['close'].rolling(30).mean()
    df_with_indicators['move_from_ma'] = df_with_indicators['move_from_ma'] / daily_params['10DATR']

    return df_with_indicators



dr = pd.date_range("2023-11-19 10:00:00", "2024-11-30 00:00:00", freq="D")
tt = tyme.time()

market_list = ['ESZ3', 'CLZ3']

for trade in todays_live_trades:
    if trade['trade_market'] not in market_list:
        print("ADDING NEW MARKET {} TO DATA STREAMING LIST".format(trade['trade_market']))
        market_list.append(trade['trade_market'])

market_order_dict = {}; ctr=0
for market in market_list:
    market_order_dict[market] = ctr; ctr+=1

market_df_list = []
for market in market_list:
    market_df_list.append(marketdatalib.read(market+'_5sec_bars', columns=None, date_range=dr))
    print("Took {} to load full {} data from db".format(tyme.time() - tt, market))

downsampled_df_list = []
for market, market_df in zip(market_list, market_df_list):
    market_df.index = market_df.index.shift(1,freq='H')
    market_df = market_df.sort_index()
    print("{} Data runs from Start: {} to End: {}".format(market, market_df.index[0], market_df.index[-1]))
    print("Total bars: {}".format(len(market_df)))

    downsampled_df_list.append(market_df.resample('1min').agg(
        {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'count': 'sum', 'volume': 'sum'}))
    openprice = get_open_price(downsampled_df_list[-1], None)
    daily_params[market]['daily_open'] = openprice

trade_metadata = {'trading_date': trading_date}
positions_metadata = {'trading_date': trading_date}
pnl_metadata = {'trading_date': trading_date}

#es_df = marketdatalib.read('ESZ3_5sec_bars', columns=None, date_range=dr)
#print("Took {} to load full data from db".format(tyme.time()-tt))
#es_df.index = es_df.index.shift(1, freq='H')
#es_df = es_df.sort_index()
#print("Start: {} to End: {}".format(es_df.index[0], es_df.index[-1]))
#print("Total bars: {}".format(len(es_df)))
#print(es_df.head(5).to_string())
#print(es_df.tail(5).to_string())
#downsampled_df = es_df.resample('1min').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'count': 'sum', 'volume': 'sum'})
#openprice = get_open_price(downsampled_df,None)
#daily_params['daily_open'] = openprice

#downsampled_df = downsampled_df.shift(-6, freq='H')
# print("{} Open price was: {}".format(openprice))
# print("Current Time is: {}".format(datetime.datetime.now()))
# print("Final bar was: ")
# print(downsampled_df.tail(2).to_string())
# most_recent_minbar = downsampled_df.index[-1]
# target_minbar = most_recent_minbar+pd.Timedelta(minutes=1)
# print("Most recent minbar: {}".format(most_recent_minbar))
#es_df = es_df.shift(6, freq='H')
#print("Last data streamed was: ")
#print(es_df.tail(1).to_string())
#last_sec_data = es_df.index[-1]

bar_tracking_df = {}
for market in market_list:
    bar_tracking_df[market] = {}

most_recent_minbar_list = []
target_minbar_list = []
for market, downsampled_df in zip(market_list,downsampled_df_list):
    print("{} Open price was: {}".format(market, daily_params[market]['daily_open']))
    print("Current Time is: {}".format(datetime.datetime.now()))
    print("Final bar for {} was: ".format(market))
    print(downsampled_df.tail(1).to_string())

    bar_tracking_df[market]['most_recent_minbar'] = downsampled_df.index[-1]
    bar_tracking_df[market]['target_minbar'] = bar_tracking_df[market]['most_recent_minbar'] + pd.Timedelta(minutes=1)

    print("Most recent {} minbar: {}".format(market,bar_tracking_df[market]['most_recent_minbar']))

tyme.sleep(1)
market_price_dict = {}
for market in market_list:
    market_price_dict[market] = 0.0

stop_flag = False
counter = 0
while not stop_flag:
    newest_data = 0

    #print("Current time is:      {}".format(datetime.datetime.now()))
    #newest_data = marketdatalib.max_date('ESZ3_5sec_bars')
    #print("Newest data in db is: {}".format(newest_data+datetime.timedelta(hours=1)))
    #print("Target minbar is: {}".format(target_minbar))
    loopt = tyme.time()
    print_df = pd.DataFrame(columns=['time','open','high','low','close','volume','range'])
    for (market, market_df, downsampled_df) in zip(market_list, market_df_list, downsampled_df_list):
        mt = tyme.time()
        if newest_data in market_df.index:
            print("We already have that data")
        else:

            now = datetime.datetime.now()-datetime.timedelta(hours=6)-datetime.timedelta(minutes=2)

            endtr = now+datetime.timedelta(days=3)
            rng = pd.date_range(start=now.strftime("%Y-%m-%d %H:%M:%S"), end=endtr.strftime("%Y-%m-%d %H:%M:%S"), freq="D")
            try:
                new_market_df = marketdatalib.read(market+'_5sec_bars', columns=None, date_range=rng)
                new_market_df.index = new_market_df.index.shift(1, freq='H')
                if len(new_market_df) > 0:
                    only_new_data = new_market_df.loc[new_market_df.index.difference(market_df.index)]
                    #print(only_new_data.to_string())
                    market_df = pd.concat([market_df, only_new_data], join='outer')
                    market_price_dict[market] = market_df['close'].iloc[-1]
                    print_df.loc[market] = [ market_df.index[-1], market_df['open'].iloc[-1],market_df['high'].iloc[-1],
                                            market_df['low'].iloc[-1],market_df['close'].iloc[-1],market_df['volume'].iloc[-1],
                                            market_df['high'].iloc[-1]-market_df['low'].iloc[-1] ]
                    #print(market_df[['open','high','low','close','count','volume']].round(2).tail(1).to_string())
                    for ix in range(0,len(only_new_data)):
                        if only_new_data.index[ix] == bar_tracking_df[market]['target_minbar']:
                            print("HIT NEW MINUTE BAR TIMESTAMP IN {}".format(market))
                            newdata = market_df.iloc[-13::]
                            newdata.drop(newdata.tail(1).index, inplace=True)
                            print(len(newdata))

                            downsampled_df.loc[bar_tracking_df[market]['most_recent_minbar']] = \
                                [newdata['open'].iloc[0],newdata['high'].max(),newdata['low'].min(),
                                 newdata['close'].iloc[-1],newdata['count'].sum(),newdata['volume'].sum()]

                            print("NEW downsampled df for {}: ".format(market))
                            print(downsampled_df.tail(1).to_string())

                            bar_tracking_df[market]['target_minbar'] = bar_tracking_df[market]['target_minbar'] + pd.Timedelta(minutes=1)
                            bar_tracking_df[market]['most_recent_minbar'] = bar_tracking_df[market]['most_recent_minbar'] + pd.Timedelta(minutes=1)
                            counter += 1
                            try:
                                for trade in todays_live_trades:
                                    if trade['trade_market'] == market:
                                        trade = on_bar_update(downsampled_df, market, daily_params[market], trade)

                            except Exception as e:
                                print(e)
                                print("Failed on bar update script")


            except Exception as e:
                print(e)
                print(downsampled_df.tail(5).to_string())
                #print([newdata['open'].iloc[0],newdata['high'].max(),newdata['low'].min(),
                                                                 #newdata['close'].iloc[-1],newdata['count'].sum(),newdata['volume'].sum()])

                assert False
        #print("Time needed for {} was {}".format(market, tyme.time()-mt))
    print(print_df.round(2).to_string())
    if any(trade['active'] for trade in todays_live_trades):
        pnl_df = pd.DataFrame(data=np.zeros((len(market_list),3)), index=market_list, columns=['Current Position',
                                                                                              'Current PnL ($)', 'Risk ($)'])
        pnl_df.loc['Total'] = [0.0, 0.0, 0.0]
        for trade in todays_live_trades:
            if trade['active']:
                pnl,risk = check_for_live_pnl(market_price_dict[trade['trade_market']], trade)
                pnl_df['Current PnL ($)'].loc[trade['trade_market']] = pnl_df['Current PnL ($)'].loc[trade['trade_market']] + pnl
                pnl_df['Current PnL ($)'].loc['Total'] = pnl_df['Current PnL ($)'].loc['Total'] + pnl

                pnl_df['Risk ($)'].loc[trade['trade_market']] = pnl_df['Risk ($)'].loc[trade['trade_market']] + risk
                pnl_df['Risk ($)'].loc['Total'] = pnl_df['Risk ($)'].loc['Total'] + risk

                pnl_df['Current Position'].loc[trade['trade_market']] = pnl_df['Current Position'].loc[trade['trade_market']] + trade['size']
                pnl_df['Current Position'].loc['Total'] = pnl_df['Current Position'].loc['Total'] + trade['size']*size_conversion_dict[trade['trade_market']]
                #check_for_live_pnl(market_df['close'].iloc[-1], market, todays_live_trades)
        #pnl_df['datetime'] = market_df.index[-1]
        #pnl_df['market'] = pnl_df.index
        #pnl_df['index'] = market_df.index[-1]
        #pnl_new_metadata = pnl_metadata['datetime'] = market_df.index[-1]
        pnl_df['market'] = pnl_df.index
        pnl_df['index'] = market_df.index[-1]
        tradinglib.write('pnl_{}'.format(trading_date), pnl_df.to_dict('records'))
        pnl_df.loc['Total (bps)'] = [0, pnl_df['Current PnL ($)'].iloc[-1]/10_000_0, pnl_df['Risk ($)'].iloc[-1]/10_000_0, 0 ,market_df.index[-1]]
        print(pnl_df.to_string())
        #tradinglib.write('pnl_{}'.format(trading_date), pnl_df.to_dict('records'))

    for trade in todays_live_trades:
        if 'exit_price' in trade.keys():
            print("REMOVING ",trade)
            todays_live_trades.remove(trade)
            print(todays_live_trades)


    while tyme.time()-loopt < 5:
        tyme.sleep(0.1)
    #print(type(tyme.time()-loopt))
    #tyme.sleep(4.8)

for market, market_df, downsampled_df in zip(market_list,market_df_list, downsampled_df_list):
    print("***********************************{}**************************".format(market))
    print(market_df.tail(12*5).to_string())

    print(downsampled_df.tail(15).to_string())




###################################### OLD VERSION SINGLE MARKET #######################################################
# stop_flag = False
# counter = 0
# while counter < 110:
#     newest_data = 0
#     #print("Current time is:      {}".format(datetime.datetime.now()))
#     #newest_data = marketdatalib.max_date('ESZ3_5sec_bars')
#     #print("Newest data in db is: {}".format(newest_data+datetime.timedelta(hours=1)))
#     #print("Target minbar is: {}".format(target_minbar))
#
#     if newest_data in es_df.index:
#         print("We already have that data")
#     else:
#         #print("Data is new")
#         now = datetime.datetime.now()-datetime.timedelta(hours=6)-datetime.timedelta(minutes=2)
#         rng = pd.date_range(start=now.strftime("%Y-%m-%d %H:%M:%S"), end="2023-11-10 00:00:00", freq="D")
#         try:
#             new_es_df = marketdatalib.read('ESZ3_5sec_bars', columns=None, date_range=rng)
#             new_es_df.index = new_es_df.index.shift(1, freq='H')
#             if len(new_es_df) > 0:
#                 only_new_data = new_es_df.loc[new_es_df.index.difference(es_df.index)]
#                 #print(only_new_data.to_string())
#                 es_df = pd.concat([es_df, only_new_data], join='outer')
#                 print(es_df.tail(1).to_string())
#                 for ix in range(0,len(only_new_data)):
#                     if only_new_data.index[ix] == target_minbar:
#                         print("HIT NEW MINUTE BAR TIMESTAMP")
#                         newdata = es_df.iloc[-13::]
#                         newdata.drop(newdata.tail(1).index, inplace=True)
#                         print(len(newdata))
#                         downsampled_df.loc[most_recent_minbar] = [newdata['open'].iloc[0],newdata['high'].max(),newdata['low'].min(),
#                                                              newdata['close'].iloc[-1],newdata['count'].sum(),newdata['volume'].sum()]
#                         print("NEW downsampled df: ")
#                         print(downsampled_df.tail(1).to_string())
#
#                         target_minbar = target_minbar + pd.Timedelta(minutes=1)
#                         most_recent_minbar = most_recent_minbar + pd.Timedelta(minutes=1)
#                         counter += 1
#                         try:
#                             todays_live_trades = on_bar_update(downsampled_df, daily_params, todays_live_trades)
#                         except Exception as e:
#                             print(e)
#                             print("Failed on bar update script")
#                 if any(trade['active'] for trade in todays_live_trades):
#                     check_for_live_pnl(es_df['close'].iloc[-1], todays_live_trades)
#
#         except Exception as e:
#             print(e)
#             print(downsampled_df.tail(5).to_string())
#             print([newdata['open'].iloc[0],newdata['high'].max(),newdata['low'].min(),
#                                                              newdata['close'].iloc[-1],newdata['count'].sum(),newdata['volume'].sum()])
#
#             assert False
#
#     tyme.sleep(5)
#
# print(es_df.tail(12*5).to_string())
#
# print(downsampled_df.tail(15).to_string())