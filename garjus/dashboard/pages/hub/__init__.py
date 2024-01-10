"""dashboard home"""
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime

import pandas as pd
import numpy as np
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import Input, Output, callback, dcc, html, dash_table as dt
import dash_bootstrap_components as dbc

from ... import utils
from ..shared import STATUS2RGB
from . import data
from ....garjus import Garjus
from .. import queue, activity

logger = logging.getLogger('dashboard.hub')


# Project table with Processing, Automations, Reports

# Bar graph of Queue
# Bar graph of Issues
# Bar graph of Activity

# Analyses graph by status? or list of active?

COMPLETE2EMO = {'0': '🔴', '1': '🟡', '2': '🟢'}


def _reports_graph(df):
    print(df)
    dfp = df.pivot_table(
        index='TYPE',
        values='VIEW',
        columns=['PROJECT'],
        aggfunc='last',
        fill_value='')

    print(dfp)

    dfp = dfp.applymap(lambda x: f'[📊]({x})' if x.startswith('http') else x)

    print(dfp)

    dfp = dfp.reset_index()
    columns = dfp.columns
    records = dfp.to_dict('records')

    # Format columns
    columns = utils.make_columns(columns)
    for i, c in enumerate(columns):
        if c['name'] not in ['TYPE']:
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    return [
        dt.DataTable(
            columns=columns,
            data=records,
            filter_action='none',
            page_action='none',
            sort_action='none',
            id='datatable-hub-reports',
            style_cell={
                'textAlign': 'center',
                'width': '10px',
                'height': 'auto',
            },
            style_header={
                'fontWeight': 'bold',
                'padding': '1px 1px 0px 1px',
            },
            fill_width=False,
        )]

def _processing_graph(df):

    df['STATUS'] = df['COMPLETE'].map(COMPLETE2EMO).fillna('')

    dfp = df.pivot_table(
                index='TYPE',
                values='STATUS',
                columns=['PROJECT'],
                aggfunc=lambda x: x.mode().iat[0],
                fill_value='')

    dfp = dfp.reset_index()
    columns = dfp.columns
    records = dfp.to_dict('records')

    return [
        dt.DataTable(
            columns=utils.make_columns(columns),
            data=records,
            filter_action='none',
            page_action='none',
            sort_action='none',
            id='datatable-hub-processing',
            #style_table={
            #    'overflowY': 'scroll',
            #    'overflowX': 'scroll',
            #},
            style_cell={
                'textAlign': 'center',
                'width': '10px',
                'height': 'auto',
            },
            style_header={
                'fontWeight': 'bold',
                'padding': '1px 1px 0px 1px',
            },
            fill_width=False,
        )]


def _automations_graph(df):

    df['STATUS'] = df['COMPLETE'].map(COMPLETE2EMO).fillna('')

    dfp = df.pivot_table(
                index='TYPE',
                values='STATUS',
                columns=['PROJECT'],
                aggfunc=lambda x: x.mode().iat[0],
                fill_value='')

    dfp = dfp.reset_index()
    columns = dfp.columns
    records = dfp.to_dict('records')

    return [
        dt.DataTable(
            columns=utils.make_columns(columns),
            data=records,
            filter_action='none',
            page_action='none',
            sort_action='none',
            id='datatable-hub-automations',
            style_cell={
                'textAlign': 'center',
                'width': '10px',
                'height': 'auto',
            },
            style_header={
                'fontWeight': 'bold',
                'padding': '1px 1px 0px 1px',
            },
            fill_width=False,
        )]


def _queue_graph(df):

    status2rgb = {k: STATUS2RGB[k] for k in queue.STATUSES}

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)

    dfp = pd.pivot_table(
        df,
        index='PROCTYPE',
        values='LABEL',
        columns=['STATUS'],
        aggfunc='count',
        fill_value=0)

    for status, color in status2rgb.items():
        ydata = sorted(dfp.index)
        if status not in dfp:
            #xdata = [0] * len(dfp.index)
            continue
        else:
            xdata = dfp[status]

        fig.append_trace(
            go.Bar(
                x=xdata,
                y=ydata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=color),
                opacity=0.9, orientation='h'),
            1,
            1
        )

    fig['layout'].update(barmode='stack', showlegend=True)

    graph = dcc.Graph(figure=fig)

    return [graph]


def _issues_graph(df):
    STATUSES = ['FAIL', 'COMPLETE', 'PASS', 'UNKNOWN']
    status2rgb = {k: STATUS2RGB[k] for k in STATUSES}
    content = []

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Draw bar for each status, these will be displayed in order
    dfp = pd.pivot_table(
        df,
        index='CATEGORY',
        values='LABEL',
        columns=['STATUS'],
        aggfunc='count',
        fill_value=0)

    for status, color in status2rgb.items():
        ydata = sorted(dfp.index)
        if status not in dfp:
            xdata = [0] * len(dfp.index)
        else:
            xdata = dfp[status]

        fig.append_trace(go.Bar(
            x=ydata,
            y=xdata,
            name='{} ({})'.format(status, sum(xdata)),
            marker=dict(color=color),
            opacity=0.9), 1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=False)

    content.append(dcc.Graph(figure=fig))

    return content


def _activity_graph(df):
    status2rgb = {k: STATUS2RGB[k] for k in activity.STATUSES}
    content = []

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Draw bar for each status, these will be displayed in order
    dfp = pd.pivot_table(
        df, index='CATEGORY', values='ID', columns=['STATUS'],
        aggfunc='count', fill_value=0)

    for status, color in status2rgb.items():
        ydata = sorted(dfp.index)
        if status not in dfp:
            xdata = [0] * len(dfp.index)
        else:
            xdata = dfp[status]

        fig.append_trace(go.Bar(
            x=ydata,
            y=xdata,
            name='{} ({})'.format(status, sum(xdata)),
            marker=dict(color=color),
            opacity=0.9), 1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=False)

    graph = dcc.Graph(figure=fig)

    content.append(graph)

    return content


def get_content():
    '''Get page content.'''

    # We use the dbc grid layout with rows and columns, rows are 12 units wide
    content = [
        dbc.Row([
            dbc.Col(dbc.Button('Refresh', id='button-hub-refresh')),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-hub-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dbc.Row([
            dbc.Col(html.Label('Activity'), width=3),
            dbc.Col(html.Label('Queue'), width=6),
            dbc.Col(html.Label('Issues'), width=3),
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(id='div-hub-activity', children=[]), width=3,
            ),
            dbc.Col(
                html.Div(id='div-hub-queue', children=[]), width=6,
            ), 
            dbc.Col(
                html.Div(id='div-hub-issues', children=[]), width=3,
            ),
        ]),
        dbc.Row([dbc.Col(html.Label('Reports'))]),
        dbc.Row([dbc.Col(html.Div(id='div-hub-reports', children=[]))]),
        dbc.Row([dbc.Col(html.Label('Processing'))]),
        dbc.Row([dbc.Col(
            html.Div(id='div-hub-processing', children=[]),
            width=12,
        )]),
        #dbc.Row([dbc.Col(html.Label('Automations'))]),
    ]

    content = [dbc.Spinner(id="loading-hub", children=content)]

    return content


@callback(
    [
     Output('dropdown-hub-proj', 'options'),
     Output('div-hub-processing', 'children'),
     Output('div-hub-reports', 'children'),
     Output('div-hub-queue', 'children'),
     Output('div-hub-issues', 'children'),
     Output('div-hub-activity', 'children'),
     ],
    [
     Input('button-hub-refresh', 'n_clicks'),
     Input('dropdown-hub-proj', 'value'),
    ],
)
def update_hub(n_clicks, selected_proj):
    refresh = False

    logger.debug('update_hub')

    if utils.was_triggered('button-hub-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh-hub:clicks={}'.format(n_clicks))
        refresh=True

    # Load datas
    queue_data = data._get_queue_data(refresh=refresh)
    issues_data = data._get_issues_data(refresh=refresh)
    act_data = data._get_activity_data(refresh=refresh)
    proc_data = data._get_processing_data()
    reports_data = data._get_reports_data(refresh=refresh)

    # Get options for dropdowns berfore filtering
    proj_options = data._load_options(proc_data)
    proj = utils.make_options(proj_options)

    # Filter data
    if selected_proj:
        proc_data = proc_data[proc_data.PROJECT.isin(selected_proj)]
        queue_data = queue_data[queue_data.PROJECT.isin(selected_proj)]
        act_data = act_data[act_data.PROJECT.isin(selected_proj)]
        issues_data = issues_data[issues_data.PROJECT.isin(selected_proj)]
        reports_data = reports_data[reports_data.PROJECT.isin(selected_proj)]

    # Make graphs/tables
    queue_graph = _queue_graph(queue_data)
    issues_graph = _issues_graph(issues_data)
    act_graph = _activity_graph(act_data)
    proc_graph = _processing_graph(proc_data)
    reports_graph = _reports_graph(reports_data)

    # Return table, figure, dropdown options
    logger.debug('update_hub:returning data')

    return [proj, proc_graph, reports_graph, queue_graph, issues_graph, act_graph]
