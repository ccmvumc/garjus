import logging

from dash import dcc, html, dash_table as dt, Input, Output, callback
import dash_bootstrap_components as dbc

from .. import utils
from . import data
from ....dictionary import COLUMNS

logger = logging.getLogger('dashboard.analyses')


COMPLETE2EMO = {'0': '🔴', '1': '🟡', '2': '🟢'}


def get_content():
    #columns = utils.make_columns(COLUMNS.get('analyses'))
    columns = utils.make_columns([
        'PROJECT',
        'ID',
        'NAME',
        'COMPLETE',
        'EDIT',
        'INPUT',
        'OUTPUT',
    ])

    # Format columns with links as markdown text
    for i, c in enumerate(columns):
        if c['name'] in ['OUTPUT', 'EDIT', 'INPUT']:
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

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
        dbc.Spinner(id="loading-analyses-table", children=[
            dbc.Label('Loading...', id='label-analyses-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='none',
            id='datatable-analyses',
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
        html.Label('0', id='label-analyses-rowcount2')]

    return content


def load_analyses(projects=[]):

    if projects is None:
        projects = []

    return data.load_data(projects, refresh=True)


@callback(
    [
     Output('dropdown-analyses-proj', 'options'),
     Output('datatable-analyses', 'data'),
     Output('label-analyses-rowcount1', 'children'),
     Output('label-analyses-rowcount2', 'children'),
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

    df['COMPLETE'] = df['COMPLETE'].map(COMPLETE2EMO).fillna('?')

    # Get the table data as one row per assessor
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        # Make edit a link
        _link = r['EDIT']
        _text = 'edit'
        r['EDIT'] = f'[{_text}]({_link})'

        # Make edit a link
        if not r['INPUT']:
            pass
        else:
            _link = r['INPUT']
            _text = r['INPUT'].rsplit('/', 1)[1]
            r['INPUT'] = f'[{_text}]({_link})'

        # Make output a link
        if not r['OUTPUT']:
            pass
        elif 'sharepoint.com' in r['OUTPUT']:
            _link = r['OUTPUT']
            _text = 'OneDrive'
            r['OUTPUT'] = f'[{_text}]({_link})'
        elif 'xnat' in r['OUTPUT']:
            _link = r['OUTPUT']
            _text = r['OUTPUT'].rsplit('/', 1)[1]
            r['OUTPUT'] = f'[{_text}]({_link})'
        else:
            r['OUTPUT'] = r['OUTPUT']

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    return [proj, records, rowcount, rowcount]
