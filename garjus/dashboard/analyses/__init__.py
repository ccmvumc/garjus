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
from . import data
from ..shared import GWIDTH


logger = logging.getLogger('dashboard.analyses')


def _plottable(var):
    try:
        _ = var.str.strip('%').astype(float)
        return True
    except Exception:
        return False


def get_graph_content(df):
    tabs_content = []
    tab_value = 0
  
    logger.debug('empty data, using empty figure')
    _txt = 'Analyses'
    return [dcc.Tab(label='', value='0', children=[html.Div(
        html.P(_txt, style={'text-align': 'center'}),
        style={'height': '150px', 'width': f'{GWIDTH}px'})])]


def get_content():

    content = [
        dcc.Loading(id="loading-analyses", children=[
            html.Div(dcc.Tabs(
                id='tabs-analyses',
                value='0',
                children=[],
                vertical=True))]),
        html.Button('Refresh Data', id='button-analyses-refresh'),
        dcc.Dropdown(
            id='dropdown-analyses-time',
            options=[
                {'label': 'all time', 'value': 'ALL'},
                {'label': '1 day', 'value': '1day'},
                {'label': '1 week', 'value': '7day'},
                {'label': '1 month', 'value': '30day'},
                {'label': '1 year', 'value': '365day'}],
            value='ALL'),
        dcc.Dropdown(
            id='dropdown-analyses-proj', multi=True,
            placeholder='Select Project(s)'),
        dt.DataTable(
            columns=[],
            data=[],
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-analyses',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
                'width': f'{GWIDTH}px'},
            style_cell={
                'textAlign': 'left',
                'padding': '5px 5px 0px 5px',
                'width': '30px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'height': 'auto',
                'minWidth': '40',
                'maxWidth': '60'},
            style_header={
                'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible'),
        html.Label('0', id='label-rowcount-analyses')]

    return content


def load_analyses(projects=[], refresh=False):

    if projects is None:
        projects = []

    return data.load_analyses(projects, refresh=refresh)


def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered
        and callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


@app.callback(
    [Output('dropdown-analyses-proj', 'options'),
     Output('datatable-analyses', 'data'),
     Output('datatable-analyses', 'columns'),
     Output('tabs-analyses', 'children'),
     Output('label-rowcount-analyses', 'children')],
    [Input('dropdown-analyses-proc', 'value'),
     Input('dropdown-analyses-proj', 'value'),
     Input('dropdown-analyses-time', 'value'),
     Input('button-analyses-refresh', 'n_clicks')])
def update_analyses(
    selected_proc,
    selected_proj,
    selected_time,
    n_clicks
):
    refresh = False

    logger.debug('update_all')

    ctx = dash.callback_context
    if was_triggered(ctx, 'button-analyses-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    # Load selected data with refresh if requested
    df = load_analyses(selected_proj, refresh=refresh)

    if selected_proj and (df.empty or (sorted(selected_proj) != sorted(df.PROJECT.unique()))):
        # A new project was selected so we force refresh
        logger.debug('new project selected, refreshing')
        df = load_analyses(selected_proj, refresh=True)

    # Get options based on selected projects, only show proc for those projects
    proj_options = data.load_options(selected_proj)

    logger.debug(f'loaded options:{proj_options}')

    proj = utils.make_options(proj_options)

    # Filter data based on dropdown values
    df = data.filter_data(df, selected_proc, selected_time)

    # Get the graph content in tabs (currently only one tab)
    tabs = get_graph_content(df)

   
    # Determine columns to be included in the table
    selected_cols = df.columns

    # Get the table data as one row per assessor
    columns = utils.make_columns(selected_cols)
    records = df.reset_index().to_dict('records')

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logger.debug('update_all:returning data')
    return [proj, records, columns, tabs, rowcount]
