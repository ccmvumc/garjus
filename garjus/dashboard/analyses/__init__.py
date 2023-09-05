import logging

from dash import dcc, html, dash_table as dt, Input, Output
import dash_bootstrap_components as dbc

from ..app import app
from .. import utils
from . import data
from ...dictionary import COLUMNS

logger = logging.getLogger('dashboard.analyses')


def get_content():
    columns = utils.make_columns(COLUMNS.get('analyses'))

    # Format columns
    for i, c in enumerate(columns):
        if c['name'] == 'OUTPUT':
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    df = load_analyses()

    content = [
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-analyses-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='native',
            id='datatable-analyses',
        #    style_table={
        #        'overflowY': 'scroll',
        #        'overflowX': 'scroll',
        #    },
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
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            export_format='xlsx',
            export_headers='names',
            export_columns='visible',
        ),
        html.Label('0', id='label-analyses-rowcount')]

    return content


def load_analyses(projects=[]):

    if projects is None:
        projects = []

    return data.load_data(projects, refresh=True)



@app.callback(
    [
    Output('dropdown-analyses-proj', 'options'),
    Output('datatable-analyses', 'data'),
    Output('label-analyses-rowcount', 'children'),
    ],
    [
    Input('dropdown-analyses-proj', 'value'),
    ])
def update_analyses(
    selected_proj,
):
    logger.debug('update_all')

    # Load selected data with refresh if requested
    df = load_analyses(selected_proj)

    # Get options based on selected projects, only show proc for those projects
    proj_options = data.load_options()

    logger.debug(f'loaded options:{proj_options}')

    proj = utils.make_options(proj_options)

    # Filter data based on dropdown values
    df = data.filter_data(df)

    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        if not r['OUTPUT']:
            continue
        if 'sharepoint.com' in r['OUTPUT']:
            _link = r['OUTPUT']
            _text = 'OneDrive'
            r['OUTPUT'] = f'[{_text}]({_link})'
        elif 'xnat' in r['OUTPUT']:
            _link = r['OUTPUT']
            _text = 'XNAT'
            r['OUTPUT'] = f'[{_text}]({_link})'
        else:
            r['OUTPUT'] = r['OUTPUT']

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [proj, records, rowcount]
