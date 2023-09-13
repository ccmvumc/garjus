import logging

import pandas as pd
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash import Input, Output
import dash_bootstrap_components as dbc

from ..app import app
from .. import utils
from ..shared import GWIDTH, STATUS2RGB
from . import data


logger = logging.getLogger('dashboard.issues')


def _get_graph_content(df):
    PIVOTS = ['PROJECT', 'CATEGORY']
    STATUSES = ['FAIL', 'COMPLETE', 'PASS', 'UNKNOWN']
    status2rgb = {k: STATUS2RGB[k] for k in STATUSES}
    tabs_content = []

    # index we are pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df,
            index=pindex,
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
        fig['layout'].update(barmode='stack', showlegend=True, width=GWIDTH)

        # Build the graph in a tab
        label = 'By {}'.format(pindex)
        graph = dcc.Graph(figure=fig)
        tab = dbc.Tab(label=label, children=[graph])

        # Append the tab
        tabs_content.append(tab)

    return dbc.Spinner(
        id="loading-issues",
        children=dbc.Tabs(id='tabs-issues', children=tabs_content))


def get_content():
    columns = utils.make_columns([
        'DATETIME',
        'CATEGORY',
        'PROJECT', 
        'SUBJECT',
        'SESSION',
        'EVENT',
        'DESCRIPTION',
    ])
    # removes: SCAN, FIELD, STATUS, ID

    # Format columns
    for i, c in enumerate(columns):
        if c['name'] in ['PROJECT']:
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'

    content = [
        dbc.Row(html.Div(id='container-issues-graph', children=[])),
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    'Refresh Data',
                    id='button-issues-refresh',
                    outline=True,
                    color='primary',
                    size='sm',
                ),
            ),
            dbc.Col(
                dbc.Switch(
                    id='switch-issues-graph',
                    label='Graph',
                    value=False,
                ),
                align='center',
            ),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-issues-project',
                    multi=True,
                    placeholder='Select Projects',
                ),
                width=4,
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-issues-category',
                    multi=True,
                    placeholder='Select Categories',
                ),
                width=4,
            ),
        ),
        dbc.Spinner(id="loading-issues-table", children=[
            dbc.Label('Loading...', id='label-issues-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            filter_action='native',
            page_action='none',
            sort_action='native',
            id='datatable-issues',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
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
                {'if': {'column_id': 'CATEGORY'}, 'textAlign': 'center'},
            ],
            style_header={
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px',
            },
            style_cell_conditional=[
                {'if': {'column_id': 'DATETIME'}, 'textAlign': 'center'},
                {'if': {'column_id': 'SUBJECT'}, 'textAlign': 'center'},
                {'if': {'column_id': 'SESSION'}, 'textAlign': 'center'},
                {'if': {'column_id': 'EVENT'}, 'textAlign': 'center'},
            ],
            css=[dict(selector="p", rule="margin: 0; text-align: center")],
            export_format='xlsx',
            export_headers='names',
            export_columns='visible',
        ),
        dbc.Label('Get ready...', id='label-issues-rowcount2'),
    ]

    return content


def load_options(df):
    options = {}

    for k in ['CATEGORY', 'PROJECT']:
        # Get a unique list of strings with blanks removed
        koptions = df[k].unique()
        koptions = [str(x) for x in koptions]
        koptions = [x for x in koptions if x]
        options[k] = sorted(koptions)

    return options


def load_issues(refresh=False):
    return data.load_data(refresh=refresh)


def filter_data(df, selected_project, selected_category):
    return data.filter_data(
        df, selected_project, selected_category)


# Issues callback
@app.callback(
    [
     Output('dropdown-issues-category', 'options'),
     Output('dropdown-issues-project', 'options'),
     Output('datatable-issues', 'data'),
     Output('container-issues-graph', 'children'),
     Output('label-issues-rowcount1', 'children'),
     Output('label-issues-rowcount2', 'children'),
    ],
    [
     Input('dropdown-issues-category', 'value'),
     Input('dropdown-issues-project', 'value'),
     Input('switch-issues-graph', 'value'),
     Input('button-issues-refresh', 'n_clicks'),
    ],
)
def update_issues(
    selected_category,
    selected_project,
    selected_graph,
    n_clicks
):
    refresh = False
    graph_content = []

    logger.debug('update_issues')

    # Load issues data
    if utils.was_triggered('button-issues-refresh'):
        # Refresh data if refresh button clicked
        logger.debug(f'issues refresh:clicks={n_clicks}')
        refresh = True

    logger.debug(f'loading issues data:refresh={refresh}')
    df = load_issues(refresh=refresh)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    options = load_options(df)
    projects = utils.make_options(options['PROJECT'])
    categories = utils.make_options(options['CATEGORY'])

    # Filter data based on dropdown values
    df = filter_data(
        df,
        selected_project,
        selected_category,
    )

    logger.debug(f'selected_graph:{selected_graph}')
    if selected_graph:
        logger.debug('getting issues graph')
        graph_content = _get_graph_content(df)

    # Get the table data
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        if r['PROJECT'] and 'PROJECTLINK' in r:
            _proj = r['PROJECT']
            _link = r['PROJECTLINK']
            r['PROJECT'] = f'[{_proj}]({_link})'

    # Return table, figure, dropdown options
    logger.debug('update_issues:returning data')

    # Count how many rows are in the table
    if len(records) > 1:
        rowcount = '{} rows'.format(len(records))
    else:
        rowcount = ''

    return [categories, projects, records, graph_content, rowcount, rowcount]
