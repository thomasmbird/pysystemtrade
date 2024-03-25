import dash,requests,pandas as pd
from dash.dependencies import Output, Input, State
from dash import dcc
from dash import html
import plotly.tools as tls
from io import StringIO
path = '/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/'
app = dash.Dash(__name__)

app.layout = html.Div([
                html.Div(['Name : ',
                          dcc.Input(id='input',value='5sec_bars_10_30_23.csv',type='text')
                          ]),
             dcc.Graph(id='price_volume'),
             dcc.Interval(id='interval-component',
                         interval=5 * 1000,  # in milliseconds
                         n_intervals=0)
             ])

# callback to store and share the df between callbacks
@app.callback(Output('price_volume', 'figure'),
        Input(component_id='input', component_property='value'),
        Input('interval-component', 'n_intervals')
        )
def update_graph(test,int):

    #p=requests.get('http://finance.google.com/finance/getprices?q='+in_data+'&x=NSE&i=61&p=1d&f=d,c,v').text
    #a=pd.read_csv(StringIO(p),skiprows=range(7),names =['date','Close','Volume'])
    df=pd.read_csv('/Users/thomasmbird/Documents/Quant Stuff/LiveMktData/ES/5sec_bars_10_30_23.csv')

    df['MMA30']=df.close.rolling(window=30).mean()

    fig = tls.make_subplots(rows=2, cols=1, shared_xaxes=True,vertical_spacing=0.009,horizontal_spacing=0.009
                            )
    fig['layout']['margin'] = {'l': 30, 'r': 10, 'b': 50, 't': 25}

    fig.append_trace({'x':df.timestamp,'y':df.close,'type':'scatter','name':'Price'},1,1)
    fig.append_trace({'x':df.timestamp,'y':df.MMA30,'type':'scatter','name':'MMA30'},1,1)
    fig.append_trace({'x':df.timestamp,'y':df.volume,'type':'bar','name':'Volume'},2,1)
    fig['layout'].update(title='1 Minute plot of ES')
    fig.update_layout(autosize=True,
                      height=600,
                      )
    print("Working!")
    return fig


if __name__ == '__main__':
    app.run_server(debug=True,port=8052)