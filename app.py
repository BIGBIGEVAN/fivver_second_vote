import json

from dash import Dash, html, dcc
from sqlalchemy import create_engine
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import datetime as dt
import dash
import dash_auth
import socket

# auth check tesing:
VALID_USERNAME_PASSWORD_PAIRS = {
    'evanchen':'evanchen',
    'jsparrow': 'jsparrow'
}

def get_layout():
    dash_layout = html.Div(children=[
        # dcc store section
        dcc.Store(id="database_data"),
        # ------------------
        # hidden input
        dcc.Input(id='default_get_data', type="hidden"),
        html.Button(id='default_test'),
        # ------------------
        html.H2('SecondVote Dashboard'),
        html.Hr(),
        html.Blockquote(children=[
            html.P("Organization"),
            dcc.Dropdown(id='organization_dropdown')
        ], style={'width': '15em', 'display': 'inline-block'}),
        dcc.Graph(id='actions_bar'),
        dcc.Graph(id='six_issue_line'),
        html.Hr(),
        html.Blockquote(children=[
            html.P("Organization(Multiple)"),
            dcc.Dropdown(id='organization_dropdown_multi', multi=True)
        ], style={'width': '15em', 'display': 'inline-block'}),
        html.Blockquote(children=[
            html.P("Issue Type"),
            dcc.Dropdown(id='issue_type_dropdown')
        ], style={'width': '15em', 'display': 'inline-block'}),
        dcc.Graph(id='mult_organization_line'),
        html.Hr(),

    ])
    return dash_layout


''''
Global variable and app setup 
'''
conn = create_engine("postgresql+psycopg2://"
                     "secondvote:VNHS)PE76a_6T7ZNBZSS"
                     "@10.10.0.207:5432/secondvotescores")
app = Dash(__name__,
           suppress_callback_exceptions=True,
           external_stylesheets=[dbc.themes.BOOTSTRAP])
auth = dash_auth.BasicAuth(
    app,
    VALID_USERNAME_PASSWORD_PAIRS
)

app.layout = get_layout
'''
end
'''

@app.callback(
    Output('database_data', 'data'),
    Output('organization_dropdown', 'options'),
    Output('organization_dropdown_multi', 'options'),
    Output('issue_type_dropdown', 'options'),
    # Output('issue_type_dropdown', 'value'),
    Input('default_get_data', 'value')
)
def get_data(default_input):
    log_df = pd.read_sql(f"SELECT * FROM log WHERE type='score'"
                         , conn)
    json_struct = json.loads(log_df.to_json(orient="records"))
    log_df_flat = pd.io.json.json_normalize(json_struct)
    organization_df = pd.read_sql_table("organization", conn)
    issue_type_df = pd.read_sql_table("issue", conn)
    merge_df = log_df_flat.merge(organization_df, how='left', left_on='new_value.parent_id',
                                 right_on='id')
    merge_df.loc[:, 'change_date'] = pd.to_datetime(merge_df.change_date, unit='ms')
    merge_df.loc[:, 'quarter'] = pd.PeriodIndex(merge_df.change_date, freq='Q')
    merge_df.loc[:, 'quarter'] = merge_df.quarter.apply( lambda x: str(x.start_time))
    organization_uni_list = sorted(organization_df.name.unique())
    return [
        merge_df.to_json(),
        organization_uni_list,
        organization_uni_list,
        ['weighted geometric mean'] + issue_type_df['name'].tolist(),
        # 'weighted geometric mean',
    ]

@app.callback(
    Output('multi_organization_line', 'figure'),
    Input('default_test', 'n_clicks'),
)
def testing_def(a):
    print('yes')
    print()
    database_df = pd.read_json(a)
    database_df = database_df.loc[database_df]
    return a

@app.callback(
    Output('actions_bar', 'figure'),
    Input('organization_dropdown', 'value'),
    State('database_data', 'data'),
    prevent_initial_callbacks=True
)
def get_actions_bar(selected_organization, database_data):
    database_df = pd.read_json(database_data)
    organization_df = database_df.loc[database_df.name == selected_organization]
    actions_bar = px.histogram(organization_df, x='quarter', y='new_value.score',
                               )
    actions_bar.update_traces(xbins_size='M3')
    actions_bar.update_xaxes(dtick='M3',
                             tickformat="Q%q\n%Y")
    actions_bar.update_layout(bargap=0.1)
    return actions_bar




if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host=socket.gethostname())

