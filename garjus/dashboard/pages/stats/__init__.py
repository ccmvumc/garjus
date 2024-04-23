import logging

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import Input, Output, callback,dcc, html, dash_table as dt
import dash_bootstrap_components as dbc

from .. import utils
from . import data
from ..shared import GWIDTH


logger = logging.getLogger('dashboard.stats')


HIDECOLS = [
    'ASSR',
    'PROJECT',
    'SUBJECT',
    'SESSION',
    'SESSIONLINK',
    'SESSTYPE',
    'SITE',
    'DATE',
    'PROCTYPE'
]


def _plottable(var):
    if pd.api.types.is_numeric_dtype(var):
        return True

    try:
        _ = var.astype(float)
        return True
    except Exception:
        pass

    try:
        _ = var.str.strip('%').astype(float)
        return True
    except Exception:
        return False


def get_graph_content(df, selected_pivot):
    tabs_content = []
    tab_value = 0
    hidecols = HIDECOLS
    logger.debug('get_stats_figure')

    # Check for empty data
    if len(df) == 0:
        logger.debug('empty data, using empty figure')
        _txt = 'Choose Project(s) then Type(s) to load stats'
        return [dcc.Tab(label='', value='0', children=[html.Div(
            html.P(_txt, style={'text-align': 'center'}),
            style={'padding':'100px', 'height': '100px', 'width': '700px'})])]

    # Filter var list to only include those that have data
    var_list = [x for x in df.columns if not pd.isnull(df[x]).all()]

    # Hide some columns
    var_list = [x for x in var_list if x not in hidecols]

    # Hide more columns
    var_list = [x for x in var_list if not (
        x.endswith('_pctused') or 
        x.endswith('_voltot')) or
        x.endswith('_volused')
    ]

    # Filter var list to only stats can be plotted as float
    var_list = [x for x in var_list if _plottable(df[x])]

    # Append the tab
    _label = 'ALL'
    _graph = get_stats_graph(df, var_list)
    _tab = dcc.Tab(label=_label, value=str(tab_value), children=[_graph])
    tabs_content.append(_tab)

    # other pivots
    pivots = ['SITE', 'PROJECT', 'SESSTYPE']
    for p in pivots:

        if len(df[p].unique()) <= 1:
            continue

        tab_value += 1
        _label = 'By ' + p
        _graph = get_stats_graph(df, var_list, p)
        _tab = dcc.Tab(label=_label, value=str(tab_value), children=[_graph])
        tabs_content.append(_tab)

    # Return the tabs
    return tabs_content


def get_stats_graph(df, var_list, pivot=None):
    box_width = 250
    min_box_count = 4

    logger.debug(f'get_graph_tab:{pivot}')

    # Determine how many boxplots we're making, depends on how many vars, use
    # minimum so graph doesn't get too small
    box_count = len(var_list)
    if box_count < min_box_count:
        box_count = min_box_count

    graph_width = box_width * box_count

    # Horizontal spacing cannot be greater than (1 / (cols - 1))
    hspacing = 1 / (box_count * 4)

    # Make the figure with 1 row and a column for each var we are plotting
    var_titles = [x[:22] for x in var_list]
    fig = plotly.subplots.make_subplots(
        rows=1,
        cols=box_count,
        horizontal_spacing=hspacing,
        subplot_titles=var_titles)

    # Draw boxplots by adding traces to figure
    for i, var in enumerate(var_list):
        _row = 1
        _col = i + 1

        # Create boxplot for this var and add to figure
        logger.debug(f'plotting var:{var}')

        # Create boxplot for this var and add to figure
        if pivot:
            _xvalues = df[pivot]
        else:
            _xvalues = None

        fig.append_trace(
            go.Box(
                y=df[var].str.strip('%').astype(float),
                x=_xvalues,
                boxpoints='all',
                text=df['ASSR'],
                boxmean=True,
            ),
            _row,
            _col)

        if var.startswith('con_') or var.startswith('inc_'):
            fig.update_yaxes(range=[-1, 1], autorange=False)
        else:
            fig.update_yaxes(autorange=True)
            pass

    # Customize figure to hide legend and fit the graph
    fig.update_layout(
        showlegend=False,
        autosize=False,
        width=graph_width,
        margin=dict(l=20, r=40, t=40, b=80, pad=0))

    if not pivot:
        fig.update_xaxes(showticklabels=False)  # hide all the xticks

    # Build the tab
    # We set the graph to overflow and then limit the size to 1000px, this
    # makes the graph stay in a scrollable section
    graph = html.Div(
        dcc.Graph(figure=fig, style={'overflow': 'scroll'}),
        style={'width': f'{GWIDTH}px'})

    # Return the graph
    return graph


def get_content():
    proj, _ = data.load_options()
    if not proj:
        logger.debug('no projects for stats')
        return None

    content = [
        dbc.Button(
            'Refresh Data',
            id='button-stats-refresh',
            outline=True,
            color='primary',
            size='sm',
        ),
        dbc.Spinner(id="loading-stats", children=[
            html.Div(dcc.Tabs(
                id='tabs-stats',
                value='0',
                children=[],
                vertical=True))]
        ),
        dbc.Row(
            dbc.Col(
                dbc.Stack([
                    dcc.Dropdown(
                        id='dropdown-stats-time',
                        options=[
                            {'label': 'all time', 'value': 'ALL'},
                            {'label': '1 day', 'value': '1day'},
                            {'label': '1 week', 'value': '7day'},
                            {'label': '1 month', 'value': '30day'},
                            {'label': '1 year', 'value': '365day'}],
                        value='ALL'
                    ),
                    dcc.Dropdown(
                        id='dropdown-stats-proj',
                        multi=True,
                        placeholder='Select Project(s)'
                    ),
                    dcc.Dropdown(
                        id='dropdown-stats-proc',
                        multi=True,
                        placeholder='Select Type(s)'
                    ),
                    dcc.Dropdown(
                        id='dropdown-stats-sess',
                        multi=True,
                        placeholder='Select Session Type(s)'
                    ),
                ]),
                width=5,
            ),
        ),
        dbc.Row(
            dbc.Col(
                dbc.RadioItems(
                    # Use specific css to make radios look like buttons
                    className="btn-group",
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary",
                    labelCheckedClassName="active",
                    options=[
                        {'label': 'Assessors', 'value': 'assr'},
                        {'label': 'Sessions', 'value': 'sess'},
                        {'label': 'Subjects', 'value': 'subj'}],
                    value='assr',
                    id='radio-stats-pivot',
                    labelStyle={'display': 'inline-block'},
                ),
                align='end',
                width=5,
            ),
        ),
        dbc.Spinner(id="loading-stats-table", children=[
            dbc.Label('Get ready...', id='label-stats-rowcount1'),
        ]),
        dt.DataTable(
            columns=[],
            data=[],
            page_action='none',
            sort_action='native',
            id='datatable-stats',
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
            style_header={
                'textAlign': 'center',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            style_cell_conditional=[
                {'if': {'column_type': 'numeric'}, 'textAlign': 'right'},
                {'if': {'column_id': 'PROJECT'}, 'textAlign': 'center'},
                {'if': {'column_id': 'SUBJECT'}, 'textAlign': 'center'},
                {'if': {'column_id': 'PROCTYPE'}, 'textAlign': 'center'},
                {'if': {'column_id': 'SESSTYPE'}, 'textAlign': 'center'},
                {'if': {'column_id': 'SITE'}, 'textAlign': 'center'},
            ],
            css=[dict(selector="p", rule="margin: 0; text-align: center")],
            export_format='xlsx',
            export_headers='names',
            export_columns='visible',
        ),
        dbc.Label('Get ready...', id='label-stats-rowcount2'),
    ]

    return content


def load_stats(projects=[], refresh=False, filename=None):

    if projects is None:
        projects = []

    return data.load_data(projects, refresh=refresh, filename=filename)


def _subject_pivot(df):
    # Pivot to one row per subject

    index_cols = ['PROJECT', 'SUBJECT', 'SITE']

    level_cols = []

    if len(df.SESSTYPE.unique()) > 1:
        level_cols.append('SESSTYPE')

    if len(df.PROCTYPE.unique()) > 1:
        level_cols.append('PROCTYPE')

    stat_cols = []

    # Drop any duplicates found
    df = df.drop_duplicates()

    # And duplicate proctype for session
    df = df.drop_duplicates(
        subset=['SUBJECT', 'SESSTYPE', 'PROCTYPE'],
        keep='last')

    df = df.drop(columns=['ASSR', 'SESSION', 'SESSIONLINK'])

    # Build list of stat columns
    stat_cols = [x for x in df.columns if (x not in index_cols and x not in ['SESSTYPE', 'PROCTYPE'])]

    # Make the pivot table based on _index, _cols, _vars
    dfp = df.pivot(index=index_cols, columns=level_cols, values=stat_cols)

    if len(level_cols) == 1:
        # Concatenate column levels to get one level with delimiter
        dfp.columns = [f'{c[1]}_{c[0]}' for c in dfp.columns.values]
    elif len(level_cols) == 2:
        # Concatenate column levels to get one level with delimiter
        dfp.columns = [f'{c[2]}_{c[1]}_{c[0]}' for c in dfp.columns.values]

    # Clear the index so all columns are named
    dfp = dfp.dropna(axis=1, how='all')
    dfp = dfp.reset_index()

    return dfp


def _session_pivot(df):
    # Pivot to one row per session
    level_cols = ['PROCTYPE']
    stat_cols = []
    index_cols = ['PROJECT', 'SUBJECT', 'SITE', 'SESSION', 'SESSIONLINK', 'SESSTYPE', 'DATE']

    # Drop any duplicates found
    df = df.drop_duplicates()

    # And drop any duplicate proctypes per session
    df = df.drop_duplicates(
        subset=['SUBJECT', 'SESSTYPE', 'PROCTYPE', 'DATE'],
        keep='last')

    df = df.drop(columns=['ASSR'])

    stat_cols = [x for x in df.columns if (x not in index_cols and x not in level_cols)]

    # Make the pivot table based on _index, _cols, _vars
    dfp = df.pivot(index=index_cols, columns=level_cols, values=stat_cols)

    dfp.columns = [c[0] for c in dfp.columns.values]

    # Clear the index so all columns are named
    dfp = dfp.dropna(axis=1, how='all')
    dfp = dfp.reset_index()

    return dfp


@callback(
    [Output('dropdown-stats-proc', 'options'),
     Output('dropdown-stats-proj', 'options'),
     Output('dropdown-stats-sess', 'options'),
     Output('datatable-stats', 'data'),
     Output('datatable-stats', 'columns'),
     Output('tabs-stats', 'children'),
     Output('label-stats-rowcount1', 'children'),
     Output('label-stats-rowcount2', 'children'),
    ],
    [
     Input('dropdown-stats-proc', 'value'),
     Input('dropdown-stats-proj', 'value'),
     Input('dropdown-stats-sess', 'value'),
     Input('dropdown-stats-time', 'value'),
     Input('radio-stats-pivot', 'value'),
     Input('button-stats-refresh', 'n_clicks'),
    ])
def update_stats(
    selected_proc,
    selected_proj,
    selected_sess,
    selected_time,
    selected_pivot,
    n_clicks
):
    refresh = False

    logger.debug('update_all')

    if utils.was_triggered('button-stats-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    # Load selected data with refresh if requested
    df = load_stats(selected_proj, refresh=refresh)

    if selected_proj and (df.empty or (sorted(selected_proj) != sorted(df.PROJECT.unique()))):
        # A new project was selected so we force refresh
        logger.debug('new project selected, refreshing')
        df = load_stats(selected_proj, refresh=True)

    # Get options based on selected projects, only show proc for those projects
    proj_options, proc_options = data.load_options(selected_proj)

    logger.debug(f'loaded options:{proj_options}:{proc_options}')

    proj = utils.make_options(proj_options)
    proc = utils.make_options(proc_options)

    # Get session types from unfiltered data
    if not df.empty:
        sess = utils.make_options(df.SESSTYPE.unique())
    else:
        sess = []

    # Filter data based on dropdown values
    df = data.filter_data(df, selected_proc, selected_time, selected_sess)

    # Get the graph content in tabs (currently only one tab)
    tabs = get_graph_content(df, selected_pivot)

    # format floats so they sort in the table
    for c in list(df.columns):
        if _plottable(df[c]):
            df[c] = df[c].str.strip('%').astype(float)

    # Apply pivot
    if selected_pivot == 'subj':
        df = _subject_pivot(df)
    elif selected_pivot == 'sess':
        df = _session_pivot(df)

    # Get the records and columns for DataTable
    _cols = [x for x in list(df.columns) if x not in ['SESSIONLINK']]
    columns = utils.make_columns(_cols)
    records = df.reset_index().to_dict('records')

    # Format records
    for r in records:
        if 'SESSION' in r and 'SESSIONLINK' in r:
            _sess = r['SESSION']
            _link = r['SESSIONLINK']
            r['SESSION'] = f'[{_sess}]({_link})'

    # Format columns
    for i, c in enumerate(columns):
        if c['name'] == 'SESSION':
            columns[i]['type'] = 'text'
            columns[i]['presentation'] = 'markdown'
        elif _plottable(df[c['name']]):
            columns[i]['type'] = 'numeric'

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logger.debug('update_all:returning data')
    return [proc, proj, sess, records, columns, tabs, rowcount, rowcount]
