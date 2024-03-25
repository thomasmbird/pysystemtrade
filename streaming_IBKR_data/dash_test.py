from dash import Dash, html, dcc, Output, Input, State, MATCH, ALL, callback
import pandas as pd
import plotly.graph_objects as go
import json
import time
import dash
from plotly.subplots import make_subplots

path = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'  # the path for the csv file

app = Dash(__name__)

app.layout = html.Div([
                dcc.Dropdown(id='dropdown1',
                             options=['5sec_bars_10_30_23'],   # name of the csv file
                             clearable=True,
                             value=[]
                             ),

                dcc.Graph(id='graph1'),
                dcc.Store(id='table1'),
                dcc.Interval(id='interval-component',
                             interval=5 * 1000,  # in milliseconds
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
    if js_df:
        df = pd.DataFrame(js_df)
        if n_clicks-1 >= len(df):
             return dash.no_update
        #if n_clicks-1 <= 0:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.03, subplot_titles=('OHLC', 'Volume'),
                            row_width=[0.2, 0.7])
        #else:

            #fig = go.Figure(**fig)
            #fig = go.Figure(**fig)
        #df = df.iloc[n_clicks-1]

        fig.add_trace(go.Candlestick(x=df[['timestamp']], open=[df['open']],
                           high=df[['high']], low=df[['low']], close=df[['close']],
                                     name="OHLC"),
                      row=1, col=1)
        fig.update(layout_xaxis_rangeslider_visible=False)
        fig.add_trace(go.Bar(x=df[['timestamp']], y=df[['volume']], showlegend=False), row=2, col=1)
        fig.update_traces(marker_color='rgba(0,0,250, 0.5)',
                          marker_line_width=0,
                          selector=dict(type="bar"))


        return fig
    return dash.no_update

if __name__ == "__main__":
    app.run(debug=True, port=8051)