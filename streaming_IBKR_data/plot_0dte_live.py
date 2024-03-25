import datetime
from ib_insync import *
import pandas as pd
import csv
import os
import glob
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
#import plotly.io as pio
#pio.renderers.default = 'svg'
pd.options.mode.chained_assignment = None

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

dir = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
file_name_base = '5sec_bars_'

files = list(filter(os.path.isfile, glob.glob(dir + "*")))
df = pd.DataFrame(index=files,columns=['mtime'])
for file in files:
    if file_name_base in file:
        df['mtime'].loc[file] = os.path.getmtime(file)

df = df.dropna(axis=0)
files = df.sort_values(by=['mtime']).iloc[-5:]

files['mtime'] = files['mtime'].apply(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))

target_file = files.index[-1]
print("Target file is: {}".format(target_file))

stop_running = False
first_run = True

while not stop_running:
    target_df = pd.read_csv(target_file,parse_dates=['timestamp'])
    target_df = target_df.set_index('timestamp')
    target_df.index = target_df.index.shift(7, freq="H")

    print(target_df.tail(5).to_string())

    #print(target_df.tail(20).to_string())

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                   vertical_spacing=0.03, subplot_titles=('OHLC', 'Volume'),
                   row_width=[0.2, 0.7])

    fig.add_trace(go.Candlestick(x=target_df.index,open=target_df['open'],
                                 high=target_df['high'],low=target_df['low'], close=target_df['close'],name="OHLC"),
                                 row=1, col=1)
    fig.update(layout_xaxis_rangeslider_visible=False)
    fig.add_trace(go.Bar(x=target_df.index, y=target_df['volume'], showlegend=False), row=2, col=1)
    fig.update_traces(marker_color='rgba(0,0,250, 0.5)',
                          marker_line_width=0,
                          selector=dict(type="bar"))
    time.sleep(10)
#fig.update_traces(marker_line_width = 0)


    fig.show()

exit()


for file in files:
    if ivoldf.empty:
        print('Creating df')
        ivoldf = pd.read_csv(file)
        ftag = file.split('.')[0].split('_')[-3:][0]+'_'+file.split('.')[0].split('_')[-3:][1]+'_'+file.split('.')[0].split('_')[-3:][2]
        print(ftag)
        ivoldf['filetime'] = ftag
        #print(file.split('.')[0].split('_')[-3:])
        print(ivoldf.shape)
    else:
        print('Loading ',file)
        ivoltempdf = pd.read_csv(file)
        ftag = file.split('.')[0].split('_')[-3:][0] + '_' + file.split('.')[0].split('_')[-3:][1] + '_' + \
               file.split('.')[0].split('_')[-3:][2]
        ivoltempdf['filetime'] = ftag
        #print(file.split('.')[0].split('_')[-3:])
        ivoldf = pd.concat([ivoldf,ivoltempdf],ignore_index=True)
        print(ivoldf.shape)

#print(ivoldf.to_string())

#fig = px.scatter(ivoldf.loc[ivoldf['putcall']=='P'],x='strike',y='bid',color='filetime')
fig = px.scatter(ivoldf,x='strike',y='last_impliedVol',color='filetime')
fig.show()


