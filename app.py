import json
from scipy.stats import gmean
from collections import Counter
from dash import Dash, html, dcc
from sqlalchemy import create_engine
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
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
        # ------------------
        #
        html.H2('SecondVote Dashboard'),
        html.Hr(),
        html.Blockquote(children=[
            html.P("Organization"),
            dcc.Loading(children=dcc.Dropdown(id='organization_dropdown'),
                        type='default')
        ], style={'width': '15em', 'display': 'inline-block'}),
        dcc.Graph(id='actions_bar'),
        html.Hr(),
        html.Blockquote(children=[
            html.P("Organization(Multiple)"),
            dcc.Loading(children=dcc.Dropdown(id='organization_dropdown_multi', multi=True),
                        type='default'),
        ], style={'width': '15em', 'display': 'inline-block'}),
        html.Blockquote(children=[
            html.P("Issue Type"),
            dcc.Loading(children=dcc.Dropdown(id='issue_type_dropdown'),
                        type='default'),
        ], style={'width': '15em', 'display': 'inline-block'}),
        dcc.Graph(id='multi_organization_line'),
        html.Hr(),
        html.Blockquote(children=[
            html.P("Organization"),
            dcc.Loading(children=dcc.Dropdown(id='six_issue_dropdown'),
                        type='default')
        ], style={'width': '15em', 'display': 'inline-block'}),
        dcc.Graph(id='six_issue_line'),
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
    Output('six_issue_dropdown', 'options'),
    Output('organization_dropdown_multi', 'options'),
    Output('issue_type_dropdown', 'options'),
    Output('issue_type_dropdown', 'value'),
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
    merge_df.rename(columns={'name': 'org_name'}, inplace=True)
    merge_df = merge_df.merge(issue_type_df, left_on='new_value.issue_id', right_on='id')
    merge_df.rename(columns={'name': 'issue_type'}, inplace=True)
    organization_uni_list = sorted(organization_df.name.unique())
    return [
        merge_df[['new_value.score', 'org_name','issue_type', 'quarter']].to_json(),
        organization_uni_list,
        organization_uni_list,
        organization_uni_list,
        ['weighted geometric mean'] + issue_type_df['name'].tolist(),
        'weighted geometric mean',
    ]


def weighted_geometric_mean(df):
    weight_count = Counter(df['new_value.score'].tolist())
    num_list = [k for k,v in weight_count.items()]
    weight_list =[v for k,v in weight_count.items()]
    wg_mean = gmean(num_list, weights=weight_list)
    return wg_mean


@app.callback(
    Output('multi_organization_line', 'figure'),
    Input('organization_dropdown_multi', 'value'),
    Input('issue_type_dropdown', 'value'),
    State('database_data', 'data')
)
def get_multi_organization_line(organizations, agg_type, database_data):
    if (organizations is None) or (agg_type is None):
        raise PreventUpdate
    database_df = pd.read_json(database_data)
    filtered_org_df = database_df.loc[database_df.org_name.isin(organizations)]
    group_df = filtered_org_df.groupby(['org_name', 'quarter'])
    wgm_df = group_df.apply(lambda x: weighted_geometric_mean(x)).reset_index(name='wgm')
    wgm_df.sort_values(by=['quarter'], inplace=True)
    multi_organization_line = px.line(wgm_df, x='quarter', y='wgm', color='org_name', markers=True)
    multi_organization_line.update_xaxes(dtick='M3',
                                        tickformat="Q%q\n%Y")
    return multi_organization_line

@app.callback(
    Output('actions_bar', 'figure'),
    Input('organization_dropdown', 'value'),
    State('database_data', 'data'),
)
def get_actions_bar(selected_organization, database_data):
    if (selected_organization is None) or (database_data is None):
        raise PreventUpdate
    database_df = pd.read_json(database_data)
    organization_df = database_df.loc[database_df.org_name == selected_organization]
    actions_bar = px.histogram(organization_df, x='quarter', y='new_value.score',
                               )
    actions_bar.update_traces(xbins_size='M3')
    actions_bar.update_xaxes(dtick='M3',
                             tickformat="Q%q\n%Y")
    actions_bar.update_layout(bargap=0.1)
    return actions_bar


@app.callback(
    Output('six_issue_line', 'figure'),
    Input('six_issue_dropdown', 'value'),
    State('database_data', 'data'),
)
def get_six_issue_line(selected_organization, database_data):
    if (selected_organization is None) or (database_data is None):
        raise PreventUpdate
    database_df = pd.read_json(database_data)
    org_df = database_df.loc[database_df.org_name == selected_organization]
    wgm_df = org_df.groupby(['org_name', 'quarter']).apply(lambda x: weighted_geometric_mean(x))
    wgm_df = wgm_df.reset_index(name='wgm')
    org_df = org_df.groupby(['quarter', 'issue_type'])['new_value.score'].sum().reset_index()
    org_df.sort_values(by=['quarter'], inplace=True)
    wgm_df.sort_values(by=['quarter'], inplace=True)
    six_issue_line = px.line(org_df, x='quarter', y='new_value.score', color='issue_type', markers=True)
    six_issue_line.add_trace(go.Scatter(x=wgm_df['quarter'],
                                        y=wgm_df['wgm'],
                                        mode='lines+markers',
                                        name='wgm',))
    six_issue_line.update_xaxes(dtick='M3',
                                tickformat="Q%q\n%Y")
    return six_issue_line

if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host=socket.gethostname())

