"""dashboard home"""
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import Input, Output, callback, dcc, html
import dash_bootstrap_components as dbc

from ... import utils
from ..shared import STATUS2RGB
from .. import queue, issues, activity
from ....garjus import Garjus


logger = logging.getLogger('dashboard.hub')


# Project table with Processing, Automations, Reports

# Bar graph of Queue
# Bar graph of Issues
# Bar graph of Activity

# Analyses graph by status?

def _get_queue_graph(clicks):
    df = queue.data.load_data(refresh=False, hidedone=True)

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


def _get_issues_graph(clicks):
    STATUSES = ['FAIL', 'COMPLETE', 'PASS', 'UNKNOWN']
    status2rgb = {k: STATUS2RGB[k] for k in STATUSES}
    content = []

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Draw bar for each status, these will be displayed in order
    df = issues.data.load_data(refresh=False)
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


def _get_activity_graph(clicks):
    status2rgb = {k: STATUS2RGB[k] for k in activity.STATUSES}
    content = []

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Draw bar for each status, these will be displayed in order
    #df = activity.data.load_data(refresh=True, )
    startdate = datetime.today() - relativedelta(days=7)
    startdate = startdate.strftime('%Y-%m-%d')

    g = Garjus()

    if not g.redcap_enabled():
        return None

    logger.info(f'loading activity:startdate={startdate}')
    df = g.activity(startdate=startdate)
    df.reset_index(inplace=True)
    df['ID'] = df.index

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
        dbc.Row([dbc.Col(html.Label('Processing'))]),
        dbc.Row([dbc.Col(html.Label('Automations'))]),
        dbc.Row([dbc.Col(html.Label('Reports'))]),
        dbc.Row([
            dbc.Col(html.Label('Activity'), width=3),
            dbc.Col(html.Label('Queue'), width=6),
            dbc.Col(html.Label('Issues'), width=3),
        ]),
        dbc.Row([
            dbc.Col(
                html.Div(id='div-hub-activity', children=[]),
                width=3,
            ),
            dbc.Col(
                html.Div(id='div-hub-queue', children=[]),
                width=6,
            ), 
            dbc.Col(
                html.Div(id='div-hub-issues', children=[]),
                width=3,
            ),
        ]),
    ]

    content = [dbc.Spinner(id="loading-hub-buttons", children=content)]

    return content


@callback(
    [
     Output('div-hub-queue', 'children'),
     Output('div-hub-issues', 'children'),
     Output('div-hub-activity', 'children'),
     ],
    [
     Input('button-hub-refresh', 'n_clicks'),
    ],
)
def update_hub(n_clicks):
    queue_graph = _get_queue_graph(n_clicks)
    issues_graph = _get_issues_graph(n_clicks)
    activity_graph = _get_activity_graph(n_clicks)

    logger.debug('update_hub')

    if utils.was_triggered('button-hub-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh-hub:clicks={}:{}'.format(n_clicks))

    # Return table, figure, dropdown options
    logger.debug('update_hub:returning data')

    return [queue_graph, issues_graph, activity_graph]
