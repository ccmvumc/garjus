import logging

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash

from ..app import app
from .. import utils
from ..shared import STATUS2HEX
from ..shared import RGB_RED, RGB_GREEN, RGB_YELLOW, RGB_GREY, RGB_BLUE, RGB_LIME, RGB_PURPLE
from . import data

logger = logging.getLogger('dashboard.queue')

STATUS2RGB = {
    'FAILED': RGB_RED,
    'COMPLETE': RGB_BLUE,
    'UPLOADING': RGB_LIME,
    'RUNNING': RGB_GREEN,
    'PENDING': RGB_YELLOW,
    'WAITING': RGB_GREY,
    'UNKNOWN': RGB_PURPLE}


def get_graph_content(df):
    PIVOTS = ['USER', 'PROJECT', 'PROCTYPE']
    status2rgb = STATUS2RGB
    tabs_content = []

    # index we are pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='LABEL', columns=['STATUS'],
            aggfunc='count', fill_value=0)

        for status, color in status2rgb.items():
            ydata = sorted(dfp.index)
            if status not in dfp:
                xdata = [0] * len(dfp.index)
            else:
                xdata = dfp[status]

            fig.append_trace(go.Bar(
                x=xdata,
                y=ydata,
                name='{} ({})'.format(status, sum(xdata)),
                marker=dict(color=color),
                opacity=0.9, orientation='h'), 1, 1)

        # Customize figure
        fig['layout'].update(barmode='stack', showlegend=True, width=900)

        # Build the tab
        label = 'By {}'.format(pindex)
        graph = html.Div(dcc.Graph(figure=fig), style={
            'width': '100%', 'display': 'inline-block'})
        tab = dcc.Tab(label=label, value=str(i + 1), children=[graph])

        # Append the tab
        tabs_content.append(tab)

    return tabs_content


def get_content():
    COLS = ['LABEL', 'STATUS', 'WALLTIME', 'MEMREQ', 'JOBID']  #, 'LASTMOD']

    df = load_data()

    graph_content = get_graph_content(df)

    # Get the rows and colums for the table
    columns = [{"name": i, "id": i} for i in COLS]
    records = df.reset_index().to_dict('records')

    queue_content = [
        dcc.Loading(id="loading-queue", children=[
            html.Div(dcc.Tabs(
                id='tabs-queue',
                value='1',
                children=graph_content,
                vertical=True))]),
        html.Button('Refresh Data', id='button-queue-refresh'),
        dcc.Dropdown(
            id='dropdown-queue-proj', multi=True,
            placeholder='Select Projects'),
        dcc.Dropdown(
            id='dropdown-queue-proc', multi=True,
            placeholder='Select Processing Types'),
        dcc.Dropdown(
            id='dropdown-queue-user', multi=True,
            placeholder='Select Users'),
        dcc.RadioItems(
            options=[
                {'label': 'Hide Done', 'value': 'HIDE'},
                {'label': 'Show Done', 'value': 'SHOW'}],
            value='HIDE',
            id='radio-queue-hidedone',
            labelStyle={'display': 'inline-block'}),
        dt.DataTable(
            cell_selectable=False,
            columns=columns,
            data=records,
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-queue',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
                'width': '900px',
            },
            style_cell={
                'textAlign': 'left',
                'padding': '5px 5px 0px 5px',
                'width': '30px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'height': 'auto',
                'minWidth': '40',
                'maxWidth': '60'},
            style_data_conditional=[
                {'if': {'column_id': 'STATUS'}, 'textAlign': 'center'},
                {'if': {'filter_query': '{STATUS} = "QUEUED"'},  'backgroundColor': STATUS2HEX['WAITING']},
                {'if': {'filter_query': '{STATUS} = "RUNNING"'},  'backgroundColor': STATUS2HEX['RUNNING']},
                {'if': {'filter_query': '{STATUS} = "WAITING"'},  'backgroundColor': STATUS2HEX['WAITING']},
                {'if': {'filter_query': '{STATUS} = "PENDING"'},  'backgroundColor': STATUS2HEX['PENDING']},
                {'if': {'filter_query': '{STATUS} = "UNKNOWN"'},  'backgroundColor': STATUS2HEX['UNKNOWN']},
                {'if': {'filter_query': '{STATUS} = "FAILED"'},   'backgroundColor': STATUS2HEX['FAILED']},
                {'if': {'filter_query': '{STATUS} = "COMPLETE"'}, 'backgroundColor': STATUS2HEX['COMPLETE']},
                {'if': {'column_id': 'STATUS', 'filter_query': '{STATUS} = ""'}, 'backgroundColor': 'white'}
            ],
            style_header={
                #'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible')]

    return queue_content


def load_data(refresh=False, hidedone=True):
    return data.load_data(refresh=refresh, hidedone=hidedone)


def load_proc_options():
    return data.load_proc_options()


def load_proj_options():
    return data.load_proj_options()


def load_user_options():
    return data.load_user_options()


def filter_data(df, selected_proj, selected_proc, selected_user):
    return data.filter_data(
        df, selected_proj, selected_proc, selected_user)


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


@app.callback(
    [Output('dropdown-queue-proc', 'options'),
     Output('dropdown-queue-proj', 'options'),
     Output('dropdown-queue-user', 'options'),
     Output('datatable-queue', 'data'),
     Output('tabs-queue', 'children')],
    [Input('dropdown-queue-proc', 'value'),
     Input('dropdown-queue-proj', 'value'),
     Input('dropdown-queue-user', 'value'),
     Input('radio-queue-hidedone', 'value'),
     Input('button-queue-refresh', 'n_clicks')])
def update_queue(
    selected_proc,
    selected_proj,
    selected_user,
    selected_done,
    n_clicks
):
    refresh = False

    logger.debug('update_queue')

    # Load data
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-queue-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('queue refresh:clicks={}'.format(n_clicks))
        refresh = True

    logger.debug('loading data')
    hidedone = (selected_done == 'HIDE')
    df = load_data(refresh=refresh, hidedone=hidedone)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proj = utils.make_options(load_proj_options())
    proc = utils.make_options(load_proc_options())
    user = utils.make_options(load_user_options())

    # Filter data based on dropdown values
    df = filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_user)

    tabs = get_graph_content(df)

    # Get the table data
    records = df.reset_index().to_dict('records')

    # Return table, figure, dropdown options
    logger.debug('update_queue:returning data')

    return [proc, proj, user, records, tabs]
