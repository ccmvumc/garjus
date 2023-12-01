import logging

from dash import dcc, html, dash_table as dt, Input, Output, callback
import dash_bootstrap_components as dbc

from .. import utils
from . import data
from ....dictionary import COLUMNS


logger = logging.getLogger('dashboard.reports')


def get_content():
    #columns = utils.make_columns(COLUMNS.get('reports'))
    columns = utils.make_columns([
        'PROJECT',
        'TYPE',
        'NAME',
        'VIEW',
        'DATE',
        'PDF',
        'DATA',
    ])

    # Format columns with links as markdown text
    for i, c in enumerate(columns):
        if c['name'] in ['VIEW']:
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    content = [
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-reports-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-reports-type',
                    multi=True,
                    placeholder='Select Types(s)',
                ),
                width=3,
            ),
            dbc.Col(
                dcc.Dropdown(
                    value='Current',
                    id='dropdown-reports-time',
                    multi=False,
                    placeholder='Select Time',
                ),
                width=3,
            ),
        ]),
        dbc.Spinner(id="loading-reports-table", children=[
            dbc.Label('Loading...', id='label-reports-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='native',
            id='datatable-reports',
            style_cell={
                'textAlign': 'center',
                'padding': '15px 5px 15px 5px',
                'height': 'auto',
            },
            style_header={
                'fontWeight': 'bold',
            },
            style_cell_conditional=[
                {'if': {'column_id': 'NAME'}, 'textAlign': 'left'},
            ],
            # Aligns the markdown cells, both vertical and horizontal
            css=[dict(selector="p", rule="margin: 0; text-align: center")],
        ),
        html.Label('0', id='label-reports-rowcount2')]

    return content


def load_reports(projects=[], types=[], timeframe=None):

    if projects is None:
        projects = []

    if types is None:
        types = []

    return data.load_data(projects, types, timeframe, refresh=True)


@callback(
    [
     Output('dropdown-reports-proj', 'options'),
     Output('dropdown-reports-type', 'options'),
     Output('dropdown-reports-time', 'options'),
     Output('datatable-reports', 'data'),
     Output('label-reports-rowcount1', 'children'),
     Output('label-reports-rowcount2', 'children'),
    ],
    [
     Input('dropdown-reports-proj', 'value'),
     Input('dropdown-reports-type', 'value'),
     Input('dropdown-reports-time', 'value'),
    ])
def update_reports(
    selected_proj,
    selected_type,
    selected_time,
):
    logger.debug('update_all')

    # Load selected data with refresh if requested
    df = load_reports(selected_proj, selected_type, selected_time)

    # Get options based on selected projects, only show proc for those projects
    projects, types, times = data.load_options(df)
    projects = utils.make_options(projects)
    types = utils.make_options(types)
    times = utils.make_options(times)

    logger.debug(f'loaded options:{projects}')
    logger.debug(f'loaded options:{types}')
    logger.debug(f'loaded options:{times}')

    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        # Make view a link
        _link = r['VIEW']
        _text = 'view'
        r['VIEW'] = f'[{_text}]({_link})'

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [projects, types, times, records, rowcount, rowcount]
