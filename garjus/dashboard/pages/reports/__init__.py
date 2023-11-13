import logging

from dash import dcc, html, dash_table as dt, Input, Output, callback
import dash_bootstrap_components as dbc

from .. import utils
from . import data
from ...dictionary import COLUMNS


logger = logging.getLogger('dashboard.report')


def get_content():
    columns = utils.make_columns(COLUMNS.get('reports'))
    
    # Format columns with links as markdown text
    for i, c in enumerate(columns):
        if c['name'] in ['OUTPUT', 'EDIT', 'INPUT']:
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    content = [
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-reports-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                ),
                width=3,
            ),
        ),
        dbc.Spinner(id="loading-reports-table", children=[
            dbc.Label('Loading...', id='label-reports-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='none',
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


def load_reports(projects=[]):

    if projects is None:
        projects = []

    return data.load_data(projects, refresh=True)


@callback(
    [
     Output('dropdown-reports-proj', 'options'),
     Output('datatable-reports', 'data'),
     Output('label-reports-rowcount1', 'children'),
     Output('label-reports-rowcount2', 'children'),
    ],
    [
     Input('dropdown-reports-proj', 'value'),
    ])
def update_reports(
    selected_proj,
):
    logger.debug('update_all')

    # Load selected data with refresh if requested
    df = load_reports(selected_proj)

    # Get options based on selected projects, only show proc for those projects
    proj_options = data.load_options()

    logger.debug(f'loaded options:{proj_options}')

    proj = utils.make_options(proj_options)

    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [proj, records, rowcount, rowcount]
