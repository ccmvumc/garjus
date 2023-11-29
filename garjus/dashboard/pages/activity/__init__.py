import logging

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash import Input, Output, callback
import dash_bootstrap_components as dbc

from .. import utils
from ...shared import GWIDTH, STATUS2RGB
from . import data


logger = logging.getLogger('dashboard.activity')


STATUSES = [
    'FAIL',
    'COMPLETE',
    'PASS',
    'UNKNOWN',
    'NQA',
    'NPUT'
]


def get_graph_content(df):
    PIVOTS = ['PROJECT', 'CATEGORY', 'SOURCE']
    status2rgb = {k: STATUS2RGB[k] for k in STATUSES}
    tabs_content = []

    # index we are pivoting on to count statuses
    for i, pindex in enumerate(PIVOTS):
        # Make a 1x1 figure
        fig = plotly.subplots.make_subplots(rows=1, cols=1)
        fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

        # Draw bar for each status, these will be displayed in order
        dfp = pd.pivot_table(
            df, index=pindex, values='ID', columns=['STATUS'],
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
        fig['layout'].update(barmode='stack', showlegend=True, width=GWIDTH)

        # Build the tab
        label = 'By {}'.format(pindex)
        graph = html.Div(dcc.Graph(figure=fig), style={
            'width': '100%', 'display': 'inline-block'})
        tab = dcc.Tab(label=label, value=str(i + 1), children=[graph])

        # Append the tab
        tabs_content.append(tab)

    # Return the tabs wrapped in a spinning loader
    return dbc.Spinner(
        id="loading-activity",
        children=[
            html.Div(
                dcc.Tabs(
                    id='tabs-activity',
                    value='1',
                    vertical=True,
                    children=tabs_content
                )
            )
        ]
    )


def get_content():

    columns = utils.make_columns([
        'ID',
        'PROJECT',
        'CATEGORY',
        'SUBJECT',
        'DATETIME',
        'DESCRIPTION',
        'SOURCE',
        'FIELD',
        'EVENT',
    ])

    content = [
        dbc.Row(html.Div(id='div-activity-graph', children=[])),
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    'Refresh Data',
                    id='button-activity-refresh',
                    outline=True,
                    color='primary',
                    size='sm',
                ),
                align='center',
            ),
            dbc.Col(
                dbc.Switch(
                    id='switch-activity-graph',
                    label='Graph',
                    value=False,
                ),
                align='center',
            ),
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-activity-project',
                    multi=True,
                    placeholder='Select Projects',
                ),
                width=4,
            ),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-activity-category',
                    multi=True,
                    placeholder='Select Categories',
                ),
                width=4,
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-activity-source',
                    multi=True,
                    placeholder='Select Sources',
                ),
                width=4,
            ),
        ),
        dbc.Spinner(id="loading-activity-table", children=[
            dbc.Label('Loading...', id='label-activity-rowcount1'),
        ]),
        dt.DataTable(
            columns=columns,
            data=[],
            page_action='none',
            sort_action='native',
            id='datatable-activity',
            style_table={
                'overflowY': 'scroll',
                'overflowX': 'scroll',
            },
            style_cell={
                'textAlign': 'center',
                'padding': '5px 5px 0px 5px',
                'width': '30px',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'height': 'auto',
                'minWidth': '40',
                'maxWidth': '70'
            },
            style_cell_conditional=[
                {'if': {'column_id': 'DESCRIPTION'}, 'textAlign': 'left'},
            ],
            style_header={
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            export_format='xlsx',
            export_headers='names',
            export_columns='visible',
        ),
        dbc.Label('Get ready...', id='label-activity-rowcount2'),
    ]

    return content


def load_activity(refresh=False):
    return data.load_data(refresh=refresh)


def load_options(df):
    options = {}

    for k in ['CATEGORY', 'SOURCE', 'PROJECT']:
        # Get a unique list of strings with blanks removed
        koptions = df[k].unique()
        koptions = [str(x) for x in koptions]
        koptions = [x for x in koptions if x]
        options[k] = sorted(koptions)

    return options


def filter_data(df, selected_project, selected_category, selected_source):
    return data.filter_data(
        df, selected_project, selected_category, selected_source)


@callback(
    [
     Output('dropdown-activity-category', 'options'),
     Output('dropdown-activity-project', 'options'),
     Output('dropdown-activity-source', 'options'),
     Output('datatable-activity', 'data'),
     Output('div-activity-graph', 'children'),
     Output('label-activity-rowcount1', 'children'),
     Output('label-activity-rowcount2', 'children'),
    ],
    [
     Input('dropdown-activity-category', 'value'),
     Input('dropdown-activity-project', 'value'),
     Input('dropdown-activity-source', 'value'),
     Input('switch-activity-graph', 'value'),
     Input('button-activity-refresh', 'n_clicks'),
    ])
def update_activity(
    selected_category,
    selected_project,
    selected_source,
    selected_graph,
    n_clicks
):
    graph_content = []
    refresh = False

    logger.debug('update_activity')

    # Load activity data
    if utils.was_triggered('button-activity-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('activity refresh:clicks={}'.format(n_clicks))
        refresh = True

    logger.debug('loading activity data')
    df = load_activity(refresh=refresh)

    # Update lists of possible options for dropdowns (could have changed),
    # make these lists before we filter what to display
    options = load_options(df)
    projects = utils.make_options(options['PROJECT'])
    categories = utils.make_options(options['CATEGORY'])
    sources = utils.make_options(options['SOURCE'])

    # Filter data based on dropdown values
    df = filter_data(
        df,
        selected_project,
        selected_category,
        selected_source)

    if selected_graph:
        graph_content = get_graph_content(df)

    # Get the table data
    records = df.reset_index().to_dict('records')

    # Count how many rows are in the table
    if len(records) > 1:
        rowcount = '{} rows'.format(len(records))
    else:
        rowcount = ''

    return [categories, projects, sources, records, graph_content, rowcount, rowcount]
