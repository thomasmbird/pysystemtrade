from dash import Dash, html, dcc, Output, Input, State, MATCH, ALL, callback
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
import dash

path = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'  # the path for the csv file

app = Dash(__name__)

app.layout = html.Div([
                dcc.Dropdown(id='dropdown1',
                             options=['5sec_bars_10_31_23'],   # name of the csv file
                             clearable=True,
                             value=[]
                             ),

                dcc.Graph(id='graph1'),
                dcc.Store(id='table1'),
                dcc.Interval(id='interval-component',
                             interval=5 * 10,  # in milliseconds
                             n_intervals=0)
])


# callback to store and share the df between callbacks
@callback(
    Output('table1', 'data'),
    Output('interval-component', 'n_intervals'),
    Input('dropdown1', 'value'),
    prevent_initial_call=True
)
def store_df(chosen_file):
    if chosen_file is None:
        return dash.no_update, dash.no_update
    else:
        df = pd.read_csv(path + chosen_file + '.csv')
        df_json = df.to_dict('records')
    return df_json, 0

@callback(
    Output('graph1', 'figure'),
    Input('interval-component', 'n_intervals'),
    State('table1', 'data'),
    State('graph1', 'figure'),
    prevent_initial_call=True
)
def callback_stats(n_clicks, js_df, fig):
    df = pd.read_csv(path + "5sec_bars_11_01_23" + '.csv')
    prev_max = len(df)
    if js_df:
        #df = pd.DataFrame(js_df)

        df = pd.read_csv(path + "5sec_bars_10_31_23" + '.csv')
        # df_json = df.to_dict('records')
        print("n_clicks is: ",n_clicks)
        #while n_clicks > len(df):
        #    df = pd.read_csv(path + "5sec_bars_10_31_23" + '.csv')
        #    print('DF Length: ', len(df))
        #    time.sleep(5)

        prev_max = len(df)

        print(len(df))
        print(n_clicks)

        if n_clicks-1 < 0:
            fig = go.Figure()
            print("CREATED FIGURE")
        else:
            print("TRYING TO LOAD FIGURE")
            fig = go.Figure(**fig)
            #print("TRYING TO LOAD FIGURE")

        #if n_clicks-1 <= 0:
        #    fig = go.Figure()
        ##else:
        #    fig = go.Figure(**fig)

        if n_clicks > len(df):
            ind = -1
        else:
            ind = n_clicks

        df = df.iloc[ind]

        #print(df.to_string())

        fig.add_trace(
            go.Candlestick(x=df[['timestamp']], open=[df['open']],
                           high=df[['high']], low=df[['low']], close=df[['close']]))

        fig.update(layout_xaxis_rangeslider_visible=False)
        #fig.add_trace(
        #    go.Scatter(x=[df['timestamp']],y=[df['close']]))
        #time.sleep(4.9)

        return fig
    return dash.no_update

if __name__ == "__main__":
    app.run(debug=True, port=8051)