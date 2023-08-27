"""qa dashboard tab.

# DESCRIPTION:
# the table is by session using a pivottable that aggregates the statuses
# for each scan/assr type. then we have dropdowns to filter by project,
# processing type, scan type, etc.
"""

# TODO: connect sessions from same subject baseline with arc line or something
# or could have "by subject" choice that has a subject per y value?

# TODO: dropdown to select from choices: last week, last month, this month.


import logging
import re
import itertools

import pandas as pd
import numpy as np
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash
import dash_bootstrap_components as dbc

from ..app import app
from .. import utils
from ..shared import QASTATUS2COLOR, RGB_DKBLUE, GWIDTH
from . import data
from ...garjus import Garjus


logger = logging.getLogger('dashboard.qa')


LEGEND1 = '''
✅QA Passed ㅤ
🟩QA TBD ㅤ
❌QA Failed ㅤ
🩷Job Failed ㅤ
🟡Needs Inputs ㅤ
🔷Job Running
□ None Found
'''

LEGEND2 = '''
🧠 MR
☢️ PET
🤯 EEG
📊 SGP
'''

MOD2EMO = {'MR': '🧠', 'PET': '☢️', 'EEG': '🤯', 'SGP': '📊'}


# The data will be pivoted by session to show a row per session and
# a column per scan/assessor type,
# the values in the column a string of characters
# that represent the status of one scan or assesor,
# the number of characters is the number of scans or assessors
# the columns will be the merged
# status column with harmonized values to be red/yellow/green/blue


def _get_graph_content(dfp):
    tabs_content = []
    tab_value = 0

    logger.debug('get_qa_figure')

    # Check for empty data
    if dfp is None or len(dfp) == 0:
        logger.debug('empty data, using empty figure')
        return [dcc.Tab(label='', value='0', children=[html.Div(
            html.H1('Choose Project(s) to load', style={'text-align': 'center'}),
            style={'padding':'150px'})])]

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # First we copy the dfp and then replace the values in each
    # scan/proc type column with a metastatus,
    # that gives us a high level status of the type for that session

    # TODO: should we just make a different pivot table here going back to
    # the original df? yes, later
    dfp_copy = dfp.copy()
    for col in dfp_copy.columns:
        if col in ('SESSION', 'PROJECT', 'DATE', 'NOTE'):
            # don't mess with these columns
            # TODO: do we need this if we haven't reindexed yet?
            continue

        # Change each value from the multiple values in concatenated
        # characters to a single overall status
        dfp_copy[col] = dfp_copy[col].apply(get_metastatus)

    # The pivot table for the graph is a pivot of the pivot table, instead
    # of having a row per session, this pivot table has a row per
    # pivot_type, we can pivot by type to get counts of each status for each
    # scan/proc type, or we can pivot by project to get counts of sessions
    # for each project
    # The result will be a table with one row per TYPE (index=TYPE),
    # and we'll have a column for each STATUS (so columns=STATUS),
    # and we'll count how many sessions (values='SESSION') we find for each
    # cell get a copy so it's defragmented
    dfp_copy = dfp_copy.reset_index().copy()

    # don't need subject
    dfp_copy = dfp_copy.drop(columns=['SUBJECT', 'SUBJECTLINK'])

    # use pandas melt function to unpivot our pivot table
    df = pd.melt(
        dfp_copy,
        id_vars=(
            'SESSION',
            'SESSIONLINK',
            'PROJECT',
            'DATE',
            'SITE',
            'SESSTYPE',
            'MODALITY',
            'NOTE'),
        value_name='STATUS')

    # We use fill_value to replace nan with 0
    dfpp = df.pivot_table(
        index='TYPE',
        columns='STATUS',
        values='SESSION',
        aggfunc='count',
        fill_value=0)

    # sort so scans are first, then assessor
    scan_type = []
    assr_type = []
    for cur_type in dfpp.index:
        # Use a regex to test if name ends with _v and a number, then assr
        if re.search('_v\d+$', cur_type):
            assr_type.append(cur_type)
        else:
            scan_type.append(cur_type)

    newindex = scan_type + assr_type
    dfpp = dfpp.reindex(index=newindex)

    # Draw bar for each status, these will be displayed in order
    # ydata should be the types, xdata should be count of status
    # for each type
    for cur_status, cur_color in QASTATUS2COLOR.items():
        ydata = dfpp.index
        if cur_status not in dfpp:
            xdata = [0] * len(dfpp.index)
        else:
            xdata = dfpp[cur_status]

        cur_name = '{} ({})'.format(cur_status, sum(xdata))

        fig.append_trace(
            go.Bar(
                x=ydata,
                y=xdata,
                name=cur_name,
                marker=dict(color=cur_color),
                opacity=0.9),
            1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=GWIDTH)

    # Build the tab
    label = 'By {}'.format('TYPE')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})

    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])

    tabs_content.append(tab)
    tab_value += 1

    # We also want a tab for By Project, so we can ask e.g. how many
    # sessions for each project, and then ask
    # which projects have a T1 and a good FS6_v1
    # later combine with other pivot
    # table and loop on pivot type
    dfpp = df.pivot_table(
        index='PROJECT',
        values='SESSION',
        aggfunc=pd.Series.nunique,
        fill_value=0)

    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    ydata = dfpp.index
    xdata = dfpp.SESSION

    cur_name = '{} ({})'.format('ALL', sum(xdata))
    cur_color = RGB_DKBLUE

    fig.append_trace(
        go.Bar(
            x=ydata,
            y=xdata,
            text=xdata,
            name=cur_name,
            marker=dict(color=cur_color),
            opacity=0.9),
        1, 1)

    # Customize figure
    fig['layout'].update(barmode='stack', showlegend=True, width=GWIDTH)

    # Build the tab
    label = 'By {}'.format('PROJECT')
    graph = html.Div(
        dcc.Graph(figure=fig),
        style={'width': '100%', 'display': 'inline-block'}
    )
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # Append the by-time graph (this was added later with separate function)
    dfs = df[['PROJECT', 'DATE', 'SESSION', 'SESSTYPE', 'SITE', 'MODALITY']].drop_duplicates()
    fig = _sessionsbytime_figure(dfs, selected_groupby='PROJECT')
    label = 'By {}'.format('TIME')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # Return the tabs wrapped in a spinning loader
    return dbc.Spinner(
        id="loading-qa",
        children=[
            html.Div(dcc.Tabs(
                id='tabs-qa',
                value='0',
                vertical=True,
                children=tabs_content))]),


def _sessionsbytime_figure(df, selected_groupby):
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    from itertools import cycle
    import plotly.express as px
    palette = cycle(px.colors.qualitative.Plotly)

    for mod, sesstype in itertools.product(df.MODALITY.unique(), df.SESSTYPE.unique()):

        # Get subset for this session type
        dfs = df[(df.SESSTYPE == sesstype) & (df.MODALITY == mod)]

        # Nothing to plot so go to next session type
        if dfs.empty:
            continue

        # Plot base on view
        view = 'default'

        if view == "month":
            pass

        elif view == 'all':
            # Let's do this for the all time view to see histograms by year
            # or quarter or whatever fits well

            # Plot this session type
            fig.append_trace(
                go.Histogram(
                    hovertext=dfs['SESSION'],
                    name='{} ({})'.format(sesstype, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs['PROJECT'],
                ),
                _row,
                _col)

        elif view == 'weekly':
            # Let's do this only for the weekly view and customize it specifically
            # for Mon thru Fri and allow you to choose this week and last week

            dfs['ONE'] = 1

            # Plot this session type
            fig.append_trace(
                go.Bar(
                    hovertext=dfs['SESSION'],
                    name='{} ({})'.format(sesstype, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs['ONE'],
                ),
                _row,
                _col)

            fig.update_layout(
                barmode='stack',
                width=GWIDTH,
                bargap=0.1)
        else:
            # Create boxplot for this var and add to figure
            # Default to the jittered boxplot with no boxes

            # markers symbols, see https://plotly.com/python/marker-style/
            if mod == 'MR':
                symb = 'circle-dot'
            elif mod == 'PET':
                symb = 'diamond-wide-dot'
            else:
                symb = 'diamond-tall-dot'

            _color = next(palette)

            # Convert hex to rgba with alpha of 0.5
            if _color.startswith('#'):
                _rgba = 'rgba({},{},{},{})'.format(
                    int(_color[1:3], 16),
                    int(_color[3:5], 16),
                    int(_color[5:7], 16),
                    0.7)
            else:
                _r, _g, _b = _color[4:-1].split(',')
                _a = 0.7
                _rgba = 'rgba({},{},{},{})'.format(_r, _g, _b, _a)

            # Plot this session type
            _row = 1
            _col = 1
            fig.append_trace(
                go.Box(
                    name='{} {} ({})'.format(sesstype, mod, len(dfs)),
                    x=dfs['DATE'],
                    y=dfs[selected_groupby],
                    boxpoints='all',
                    jitter=0.7,
                    text=dfs['SESSION'],
                    pointpos=0.5,
                    orientation='h',
                    marker={
                        'symbol': symb,
                        'color': _rgba,
                        'size': 12,
                        'line': dict(width=2, color=_color)
                    },
                    line={'color': 'rgba(0,0,0,0)'},
                    fillcolor='rgba(0,0,0,0)',
                    hoveron='points',
                ),
                _row,
                _col)

            # show lines so we can better distinguish categories
            fig.update_yaxes(showgrid=True)

            x_mins = []
            x_maxs = []
            for trace_data in fig.data:
                x_mins.append(min(trace_data.x))
                x_maxs.append(max(trace_data.x))

            x_min = min(x_mins)
            x_max = max(x_maxs)

            if x_min == '2021-11-01' or x_min == '2021-11-10':
                fig.update_xaxes(
                    range=('2021-10-31', '2021-12-01'),
                    tickvals=[
                        '2021-11-01',
                        '2021-11-08',
                        '2021-11-15',
                        '2021-11-22',
                        '2021-11-29'])

            fig.update_layout(width=GWIDTH)

    return fig


def get_content():
    '''Get QA page content.'''

    graph_content = _get_graph_content(None)

    favorites = Garjus().favorites()

    # We use the dbc grid layout with rows and columns, rows are 12 units wide
    content = [
        dbc.Row(html.Div(id='div-qa-graph', children=[])),
        dbc.Row([
            dbc.Col(
                dcc.DatePickerRange(id='dpr-qa-time', clearable=True),
                width=5,
            ),
            dbc.Col(
                dbc.Button(
                    'Refresh Data',
                    id='button-qa-refresh',
                ),
                align='end',
            ),
            dbc.Col(
                dbc.Switch(
                    id='switch-qa-autofilter',
                    label='Autofilter',
                    value=True,
                ), 
                align='end',
            ),
            dbc.Col(
                dbc.Switch(
                    id='switch-qa-graph',
                    label='Graph',
                    value=False,
                ),
                align='end',
            ),
        ]),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-qa-proj',
                    multi=True,
                    placeholder='Select Project(s)',
                    value=favorites,
                ),
                width=5,
            ),
        ),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-qa-sess',
                    multi=True,
                    placeholder='Select Session Type(s)',
                ),
                width=5,
            ),
            dbc.Col(
                dbc.Checklist(
                    options=[
                        {'label': '🧠 MR', 'value': 'MR'},
                        {'label': '☢️ PET', 'value': 'PET'},
                        {'label': '🤯 EEG', 'value': 'EEG'},
                        {'label': '📊 SGP', 'value': 'SGP'},
                    ],
                    value=['MR', 'PET', 'EEG'],
                    id='switches-qa-modality',
                    inline=True,
                    switch=True
                ),
            ),
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-qa-proc',
                    multi=True,
                    placeholder='Select Processing Type(s)',
                ),
                width=5,
            ),
            dbc.Col(
                dbc.Checklist(
                    options=[
                        {'label': '✅', 'value': 'P'},
                        {'label': '🟩', 'value': 'Q'},
                        {'label': '❌', 'value': 'F'},
                        {'label': '🩷', 'value': 'X'},
                        {'label': '🟡', 'value': 'N'},
                        {'label': '🔷', 'value': 'R'},
                        {'label': '□', 'value': 'E'},
                    ],
                    value=['P', 'Q', 'F', 'X', 'N', 'R', 'E'],
                    id='switches-qa-procstatus',
                    inline=True,
                    switch=True
                ),
            ),
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id='dropdown-qa-scan',
                    multi=True,
                    placeholder='Select Scan Type(s)',
                ),
                width=5,
            ),
        ]),
        dbc.Row([
            dbc.Col(
                dbc.RadioItems(
                    # Use specific css to make radios look like buttons
                    className="btn-group",
                    inputClassName="btn-check",
                    labelClassName="btn btn-outline-primary",
                    labelCheckedClassName="active",
                    options=[
                        {'label': 'Sessions', 'value': 'sess'},
                        {'label': 'Subjects', 'value': 'subj'},
                        {'label': 'Projects', 'value': 'proj'},
                    ],
                    value='sess',
                    id='radio-qa-pivot',
                    labelStyle={'display': 'inline-block'},
                ),
                align='end',
                width=5,
            ),
        ]),
        dbc.Spinner(id="loading-qa-table", children=[
            dbc.Label('Loading table...', id='label-qa-rowcount1'),
        ]),
        dt.DataTable(
                columns=[],
                data=[],
                filter_action='native',
                page_action='none',
                sort_action='native',
                id='datatable-qa',
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
                style_header={
                    'fontWeight': 'bold',
                    'padding': '5px 15px 0px 10px',
                },
                style_cell_conditional=[
                    {'if': {'column_id': 'NOTE'}, 'textAlign': 'left'},
                    {'if': {'column_id': 'SESSIONS'}, 'textAlign': 'left'},
                    {'if': {'column_id': 'SESSION'}, 'textAlign': 'center'},
                ],
                css=[dict(selector= "p", rule= "margin: 0; text-align: center")],
                fill_width=False,
                export_format='xlsx',
                export_headers='names',
                export_columns='visible'
            ),
            dbc.Label('Loading table...', id='label-qa-rowcount2'),
        html.Div([
            html.P(
                LEGEND1,
                style={'marginTop': '15px', 'textAlign': 'center'}
            ),
            html.P(
                LEGEND2,
                style={'textAlign': 'center'}
            )],
            style={'textAlign': 'center'}),
    ]

    return html.Div(content, className='dbc', style={'marginLeft': '10px'})


def get_metastatus(status):

    if status != status:
        # empty so it's none
        metastatus = 'NONE'
    elif not status or pd.isnull(status):
        # empty so it's none
        metastatus = 'NONE'
    elif 'P' in status:
        # at least one passed, so PASSED
        metastatus = 'PASS'
    elif 'Q' in status:
        # any are still needs qa, then 'NEEDS_QA'
        metastatus = 'NQA'
    elif 'N' in status:
        # if any jobs are still running, then NEEDS INPUTS?
        metastatus = 'NPUT'
    elif 'F' in status:
        # at this point if one failed, then they all failed, so 'FAILED'
        metastatus = 'FAIL'
    elif 'X' in status:
        metastatus = 'JOBF'
    elif 'R' in status:
        metastatus = 'JOBR'
    else:
        # whatever else is UNKNOWN, grey
        metastatus = 'NONE'

    return metastatus


def qa_pivot(df):
    df.DATE = df.DATE.fillna('')

    dfp = df.pivot_table(
        index=(
            'SESSION', 'SESSIONLINK', 'SUBJECT', 'SUBJECTLINK', 'PROJECT',
            'DATE', 'SESSTYPE', 'SITE', 'MODALITY', 'NOTE'),
        columns='TYPE',
        values='STATUS',
        aggfunc=lambda x: ''.join(x))

    # and return our pivot table
    return dfp


# This is where the data gets initialized
def load_data(projects=[], refresh=False, hidetypes=True, hidesgp=False):
    if projects is None:
        projects = []

    return data.load_data(
        projects=projects,
        refresh=refresh,
        hidetypes=hidetypes,
        hidesgp=hidesgp)


def load_options(projects):
    return data.load_options(projects)

def was_triggered(callback_ctx, button_id):
    result = (
        callback_ctx.triggered and
        callback_ctx.triggered[0]['prop_id'].split('.')[0] == button_id)

    return result


# Initialize the callbacks for the app

# inputs:
# values from assr proc types dropdown
# values from project dropdown
# values from timeframe dropdown
# number of clicks on refresh button

# returns:
# options for the assessor proc types dropdown
# options for the assessor projects dropdown
# options for the assessor scans dropdown
# options for the assessor sessions dropdown
# data for the table
# content for the graph tabs
@app.callback(
    [Output('dropdown-qa-proc', 'options'),
     Output('dropdown-qa-scan', 'options'),
     Output('dropdown-qa-sess', 'options'),
     Output('dropdown-qa-proj', 'options'),
     Output('datatable-qa', 'data'),
     Output('datatable-qa', 'columns'),
     Output('div-qa-graph', 'children'),
     Output('label-qa-rowcount1', 'children'),
     Output('label-qa-rowcount2', 'children'),
     ],
    [Input('dropdown-qa-proc', 'value'),
     Input('dropdown-qa-scan', 'value'),
     Input('dropdown-qa-sess', 'value'),
     Input('dropdown-qa-proj', 'value'),
     Input('dpr-qa-time', 'start_date'),
     Input('dpr-qa-time', 'end_date'),
     Input('switch-qa-autofilter', 'value'),
     Input('switch-qa-graph', 'value'),
     Input('switches-qa-procstatus', 'value'),
     Input('switches-qa-modality', 'value'),
     Input('radio-qa-pivot', 'value'),
     Input('button-qa-refresh', 'n_clicks')])
def update_all(
    selected_proc,
    selected_scan,
    selected_sess,
    selected_proj,
    selected_starttime,
    selected_endtime,
    selected_autofilter,
    selected_graph,
    selected_procstatus,
    selected_modality,
    selected_pivot,
    n_clicks
):
    tabs = []
    refresh = False

    logger.debug('update_all')

    # Load. This data will already be merged scans and assessors, row per
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-qa-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    logger.info(f'loading data:{selected_proj}')
    df = load_data(
        projects=selected_proj,
        refresh=refresh,
        hidetypes=selected_autofilter)

    if selected_proj and (df.empty or (sorted(selected_proj) != sorted(df.PROJECT.unique()))):
        # A new project was selected so we force refresh
        logger.debug('new project selected, refreshing')
        df = load_data(selected_proj, refresh=True)

    # Truncate NOTE
    if 'NOTE' in df:
        df['NOTE'] = df['NOTE'].str.slice(0, 70)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proj, sess, proc, scan = load_options(selected_proj)

    # Remove from selected what is no longer an option
    if selected_sess:
        selected_sess = [x for x in selected_sess if x in sess]
    if selected_proc:
        selected_proc = [x for x in selected_proc if x in proc]
    if selected_scan:
        selected_scan = [x for x in selected_scan if x in scan]

    # Convert to dash options
    proj = utils.make_options(proj)
    sess = utils.make_options(sess)
    proc = utils.make_options(proc)
    scan = utils.make_options(scan)

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_scan,
        selected_starttime,
        selected_endtime,
        selected_sess)

    if not df.empty and selected_modality:
        df = df[df.MODALITY.isin(selected_modality)]

    if not df.empty and selected_procstatus:
        df = df[df.STATUS.isin(selected_procstatus)]

    if df.empty:
         records = []
         columns = []
    elif selected_pivot == 'proj':
        # Get the qa pivot from the filtered data
        dfp = qa_pivot(df)

        if selected_graph:
            # Graph it
            tabs = _get_graph_content(dfp)

        # Make the table data
        selected_cols = ['PROJECT']

        dfp = dfp.reset_index()

        if selected_proc:
            selected_cols += selected_proc
            show_proc = [x for x in selected_proc if x in dfp.columns]
        else:
            show_proc = []

        if selected_scan:
            selected_cols += selected_scan
            show_scan = [x for x in selected_scan if x in dfp.columns]
        else:
            show_scan = []

        show_col = show_proc + show_scan

        if show_col:
            # aggregrate to most common value (mode)
            dfp = dfp.pivot_table(
                index=('PROJECT'),
                values=show_col,
                aggfunc=pd.Series.mode)

            for p in show_col:
                dfp[p] = dfp[p].str.replace('P', '✅')
                dfp[p] = dfp[p].str.replace('X', '🩷')
                dfp[p] = dfp[p].str.replace('Q', '🟩')
                dfp[p] = dfp[p].str.replace('N', '🟡')
                dfp[p] = dfp[p].str.replace('R', '🔷')
                dfp[p] = dfp[p].str.replace('F', '❌')
                if 'E' in selected_procstatus:
                    dfp[p] = dfp[p].fillna('□')

            # Drop empty rows
            dfp = dfp.dropna(subset=show_col)
        else:
            # No types selected, show sessions concat
            selected_cols += ['SESSIONS']
            dfp = dfp.sort_values('MODALITY')
            dfp['SESSIONS'] = dfp['MODALITY'].map(MOD2EMO).fillna('?')

            dfp = dfp[['PROJECT', 'SESSIONS', 'SESSTYPE']].drop_duplicates()

            dfp = dfp.pivot_table(
                index=('PROJECT'),
                values='SESSIONS',
                aggfunc=lambda x: ''.join(x))

        # Format as column names and record dictionaries for dash table
        columns = utils.make_columns(selected_cols)
        records = dfp.reset_index().to_dict('records')

    elif selected_pivot == 'subj':
        # row per subject

        # Get the qa pivot from the filtered data
        dfp = qa_pivot(df)

        if selected_graph:
            # Make graphs
            tabs = _get_graph_content(dfp)

        # Get the table
        dfp = dfp.reset_index()

        selected_cols = ['PROJECT', 'SUBJECT']

        if selected_proc:
            selected_cols += selected_proc
            show_proc = [x for x in selected_proc if x in dfp.columns]
        else:
            show_proc = []

        if selected_scan:
            #cols += selected_scan
            selected_cols += selected_scan
            show_scan = [x for x in selected_scan if x in dfp.columns]
        else:
            show_scan = []

        show_col = show_proc + show_scan

        if show_col:
            # append sess type to proctype/scantype columns
            # before agg so we get a column per sesstype
            # but only if there are fewer than 10 session types
            typecount = len(dfp.SESSTYPE.unique())
            if typecount < 10:
                # agg to most common value (mode) per sesstype per show_col
                dfp = dfp.pivot_table(
                    index=('PROJECT', 'SUBJECT', 'SUBJECTLINK'),
                    columns='SESSTYPE',
                    values=show_col,
                    aggfunc=pd.Series.mode,
                    fill_value='',
                )

                if typecount > 1:
                    # Concatenate column levels to get one level with delimiter
                    dfp.columns = [f'{c[1]}_{c[0]}' for c in dfp.columns.values]
                else:
                    dfp.columns = [c[0] for c in dfp.columns.values]

                # Drop empty
                for p in dfp.columns:
                    dfp[p] = dfp[p].astype(str).str.replace('[]', '', regex=False)

                dfp = dfp.replace(r'^\s*$', np.nan, regex=True)
                dfp = dfp.dropna(axis=1, how='all')

                # Save the list columns before we reset index
                show_col = list(dfp.columns)

                # Clear the index so all columns are named
                dfp = dfp.reset_index()
            else:
                # aggregrate to most common value (mode)
                dfp = dfp.pivot_table(
                    index=('PROJECT', 'SUBJECT', 'SUBJECTLINK'),
                    values=show_col,
                    aggfunc=pd.Series.mode,
                )

            for p in show_col:
                dfp[p] = dfp[p].str.replace('P', '✅')
                dfp[p] = dfp[p].str.replace('X', '🩷')
                dfp[p] = dfp[p].str.replace('Q', '🟩')
                dfp[p] = dfp[p].str.replace('N', '🟡')
                dfp[p] = dfp[p].str.replace('R', '🔷')
                dfp[p] = dfp[p].str.replace('F', '❌')
                if 'E' in selected_procstatus:
                    dfp[p] = dfp[p].fillna('□')

            # Drop empty rows
            dfp = dfp.dropna(subset=show_col)

            selected_cols = ['PROJECT', 'SUBJECT'] + show_col
        else:
            # No types selected so show session types by modality

            dfp = dfp.sort_values('MODALITY')

            dfp['EMO'] = dfp['MODALITY'].map(MOD2EMO).fillna('?')

            # Pivot to column for each session type
            show_col = list(dfp.SESSTYPE.unique())
            selected_cols = ['SUBJECT', 'PROJECT'] + show_col
            dfp = dfp.pivot_table(
                index=('SUBJECT', 'PROJECT', 'SUBJECTLINK'),
                values='EMO',
                columns='SESSTYPE',
                aggfunc=lambda x: ''.join(x))

            for p in show_col:
                if 'E' in selected_procstatus:
                    dfp[p] = dfp[p].fillna('□')

        # Format as column names and record dictionaries for dash table
        columns = utils.make_columns(selected_cols)
        records = dfp.reset_index().to_dict('records')

        # Format records
        for r in records:
            if r['SUBJECT'] and 'SUBJECTLINK' in r:
                _subj = r['SUBJECT']
                _link = r['SUBJECTLINK']
                r['SUBJECT'] = f'[{_subj}]({_link})'

        # Format columns
        for i, c in enumerate(columns):
            if c['name'] in ['SESSION', 'SUBJECT']:
                columns[i]['type'] = 'text'
                columns[i]['presentation'] = 'markdown'
    else:
        # Get the qa pivot from the filtered data
        dfp = qa_pivot(df)
        if selected_graph:
            tabs = _get_graph_content(dfp)

        # Get the table data
        selected_cols = [
            'SESSION', 'SUBJECT', 'PROJECT', 'DATE', 'SESSTYPE', 'SITE']

        if selected_proc:
            selected_cols += selected_proc
            show_proc = [x for x in selected_proc if x in dfp.columns]
        else:
            show_proc = []

        if selected_scan:
            selected_cols += selected_scan
            show_scan = [x for x in selected_scan if x in dfp.columns]
        else:
            show_scan = []

        show_col = show_proc + show_scan

        for p in show_col:
            # Replace letters with emojis
            dfp[p] = dfp[p].str.replace('P', '✅')
            dfp[p] = dfp[p].str.replace('X', '🩷')
            dfp[p] = dfp[p].str.replace('Q', '🟩')
            dfp[p] = dfp[p].str.replace('N', '🟡')
            dfp[p] = dfp[p].str.replace('R', '🔷')
            dfp[p] = dfp[p].str.replace('F', '❌')
            if 'E' in selected_procstatus:
                dfp[p] = dfp[p].fillna('□')

        # Drop empty rows
        dfp = dfp.dropna(subset=show_col)

        # Final column is always notes
        selected_cols.append('NOTE')

        # Format as column names and record dictionaries for dash table
        columns = utils.make_columns(selected_cols)
        records = dfp.reset_index().to_dict('records')

        # Format records
        for r in records:
            if r['SESSION'] and 'SESSIONLINK' in r:
                _sess = r['SESSION']
                _link = r['SESSIONLINK']
                r['SESSION'] = f'[{_sess}]({_link})'

            if r['SUBJECT'] and 'SUBJECTLINK' in r:
                _subj = r['SUBJECT']
                _link = r['SUBJECTLINK']
                r['SUBJECT'] = f'[{_subj}]({_link})'

        # Format columns
        for i, c in enumerate(columns):
            if c['name'] in ['SESSION', 'SUBJECT']:
                columns[i]['type'] = 'text'
                columns[i]['presentation'] = 'markdown'

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logger.debug('update_all:returning data')
    return [proc, scan, sess, proj, records, columns, tabs, rowcount, rowcount]
