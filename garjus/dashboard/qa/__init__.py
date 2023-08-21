"""qa.

# DESCRIPTION:
# the table is by session using a pivottable that aggregates the statuses
# for each scan/assr type. then we have dropdowns to filter by project,
# processing type, scan type, etc.
"""

import logging
import re
import itertools

import pandas as pd
import plotly
import plotly.graph_objs as go
import plotly.subplots
from dash import dcc, html, dash_table as dt
from dash.dependencies import Input, Output
import dash

from ..app import app
from .. import utils
from ..shared import QASTATUS2COLOR, RGB_DKBLUE, GWIDTH
from . import data


logger = logging.getLogger('dashboard.qa')


LEGEND1 = '''
✅QA Passed ㅤ
🟩QA TBD ㅤ 
❌QA Failed ㅤ 
🩷Job Failed ㅤ 
🟡Needs Inputs ㅤ 
🔷Job Running
'''

LEGEND2 = '''
🧠 MRI
☢️ PET
'''

MOD2EMO = {'EEG': '🤯', 'MR': '🧠',  'PET': '☢️'}


def _get_graph_content(dfp, selected_groupby='PROJECT'):
    tabs_content = []
    tab_value = 0

    logger.debug('get_qa_figure')

    # Make a 1x1 figure
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # Check for empty data
    if len(dfp) == 0:
        logger.debug('empty data, using empty figure')
        return [fig]

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
    # cell
    # get a copy so it's defragmented
    dfp_copy = dfp_copy.reset_index().copy()

    # don't need subject
    dfp_copy = dfp_copy.drop(columns=['SUBJECT'])

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
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # Append the by-time graph (this was added later with separate function)
    dfs = df[['PROJECT', 'DATE', 'SESSION', 'SESSTYPE', 'SITE', 'MODALITY']].drop_duplicates()
    fig = _sessionsbytime_figure(dfs, selected_groupby)
    label = 'By {}'.format('TIME')
    graph = html.Div(dcc.Graph(figure=fig), style={
        'width': '100%', 'display': 'inline-block'})
    tab = dcc.Tab(label=label, value=str(tab_value), children=[graph])
    tabs_content.append(tab)
    tab_value += 1

    # TODO: write a "graph in a tab" function to wrap each figure above
    # in a graph in a tab, b/c DRY

    # Return the tabs
    return tabs_content


def _sessionsbytime_figure(df, selected_groupby):
    fig = plotly.subplots.make_subplots(rows=1, cols=1)
    fig.update_layout(margin=dict(l=40, r=40, t=40, b=40))

    # TODO: if weekly is chosen, show the actual session name instead of a dot

    # TODO: use different shapes for PET vs MR

    # TODO: try to connect baseline with followup with arc line or something
    # or could have "by subject" choice that has a subject per y value

    # Customize figure
    # fig['layout'].update(xaxis={'automargin': True}, yaxis={'automargin': True})

    from itertools import cycle
    import plotly.express as px
    palette = cycle(px.colors.qualitative.Plotly)
    # palette = cycle(px.colors.qualitative.Vivid)
    # palette = cycle(px.colors.qualitative.Bold)

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
    df = data.load_data(hidetypes=True)

    if df.empty:
        return ''

# The data will be pivoted by session to show a row per session and
# a column per scan/assessor type,
# the values in the column a string of characters
# that represent the status of one scan or assesor,
# the number of characters is the number of scans or assessors
# the columns will be the merged
# status column with harmonized values to be red/yellow/green/blue

    dfp = qa_pivot(df)

    qa_graph_content = _get_graph_content(dfp)

    # Get the rows and colums for the table
    qa_columns = [{"name": i, "id": i} for i in dfp.index.names]
    dfp.reset_index(inplace=True)
    qa_data = dfp.to_dict('records')

    qa_content = [
        dcc.Loading(id="loading-qa", children=[
            html.Div(dcc.Tabs(
                id='tabs-qa',
                value='0',
                vertical=True,
                children=qa_graph_content))]),
        html.Button('Refresh Data', id='button-qa-refresh'),
        html.Div([
            #html.Div(
                #dcc.Dropdown(
                #    id='dropdown-qa-time',
                #    # TODO: Change filters to "Today", "this week", "last week",
                #    # "this month", "last month", "YTD", "past year", "last year"
                #    options=[
                #        {'label': 'all time', 'value': 'ALL'},
                #        {'label': '1 day', 'value': '1day'},
                #        {'label': '1 week', 'value': '7day'},
                #        {'label': '1 month', 'value': '30day'},
                #        #{'label': 'this week', 'value': 'thisweek'},
                #        #{'label': 'this month', 'value': 'thismonth'},
                #        {'label': 'last month', 'value': 'lastmonth'},
                #        {'label': '1 year', 'value': '365day'}],
                #    value='ALL',
                #    style={'width': '100%'}),
                #style={'width': '15%'}
            #),
            dcc.DatePickerRange(
                id='dpr-qa-time',
                clearable=True,
            ),
            dcc.RadioItems(
                options=[
                    {'label': 'Group by Project', 'value': 'PROJECT'},
                    {'label': 'Group by Site', 'value': 'SITE'}],
                value='PROJECT',
                id='radio-qa-groupby',
                labelStyle={'display': 'inline-block', "align-items": "center"},
                style={'width': '0%', 'text-align': 'center', "align-items": "center", 'display': 'none'},
            ),
            dcc.RadioItems(
                options=[
                    {'label': 'Hide Unused', 'value': 'HIDE'},
                    {'label': 'Show All Types', 'value': 'SHOW'}],
                value='HIDE',
                id='radio-qa-hidetypes',
                labelStyle={'display': 'inline-block', "align-items": "center"}, 
                style={'width': '25%', 'text-align': 'center', "align-items": "center"}),
        ], style={"display": "inline-flex", "width": "100%"}),
        dcc.Dropdown(
            id='dropdown-qa-proj', multi=True, 
            placeholder='Select Project(s)', style={'width': '70%'}),
        dcc.Dropdown(
            id='dropdown-qa-sess', multi=True,
            placeholder='Select Session Type(s)', style={'width': '70%'}),
        dcc.Dropdown(
            id='dropdown-qa-proc', multi=True,
            placeholder='Select Processing Type(s)', style={'width': '70%'}),
        dcc.Dropdown(
            id='dropdown-qa-scan', multi=True,
            placeholder='Select Scan Type(s)', style={'width': '70%'}),
        dcc.RadioItems(
            options=[
                {'label': 'Row per Session', 'value': 'sess'},
                {'label': 'Row per Subject', 'value': 'subj'},
                {'label': 'Row per Project', 'value': 'proj'},
                ],
            value='sess',
            id='radio-qa-pivot',
            labelStyle={'display': 'inline-block'}),
        dt.DataTable(
            columns=qa_columns,
            data=qa_data,
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
                'maxWidth': '70'},
            style_header={
                #'width': '80px',
                'backgroundColor': 'white',
                'fontWeight': 'bold',
                'padding': '5px 15px 0px 10px'},
            style_cell_conditional=[
                {'if': {'column_id': 'NOTE'}, 'textAlign': 'left'},
                {'if': {'column_id': 'SESSIONS'}, 'textAlign': 'left'}
            ],
            fill_width=False,
            export_format='xlsx',
            export_headers='names',
            export_columns='visible'),
        html.Label('0', id='label-qa-rowcount'),
        html.Div([
            html.P(LEGEND1, style={'marginTop': '15px', 'textAlign': 'center'}), 
            html.P(LEGEND2, style={'textAlign': 'center'})],
            style={'textAlign': 'center'}),
    ]

    return qa_content


def get_metastatus(status):

    if status != status:
        # empty so it's none
        metastatus = 'NONE'
    elif not status or pd.isnull(status):  # np.isnan(status):
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
    dfp = df.pivot_table(
        index=(
            'SESSION', 'SESSIONLINK', 'SUBJECT', 'PROJECT',
            'DATE', 'SESSTYPE', 'SITE', 'MODALITY', 'NOTE'),
        columns='TYPE',
        values='STATUS',
        aggfunc=lambda x: ''.join(x))

    # and return our pivot table
    return dfp


# This is where the data gets initialized
def load_data(refresh=False, hidetypes=True, hidesgp=False):
    return data.load_data(
        refresh=refresh, hidetypes=hidetypes, hidesgp=hidesgp)


def load_proj_options():
    return data.load_proj_options()


def load_sess_options(proj_filter=None):
    return data.load_sess_options(proj_filter)


def load_scan_options(proj_filter=None):
    return data.load_scan_options(proj_filter)


def load_proc_options(proj_filter=None):
    return data.load_proc_options(proj_filter)


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
     Output('tabs-qa', 'children'),
     Output('label-qa-rowcount', 'children'),
     ],
    [Input('dropdown-qa-proc', 'value'),
     Input('dropdown-qa-scan', 'value'),
     Input('dropdown-qa-sess', 'value'),
     Input('dropdown-qa-proj', 'value'),
     Input('dpr-qa-time', 'start_date'),
     Input('dpr-qa-time', 'end_date'),
     Input('radio-qa-groupby', 'value'),
     Input('radio-qa-hidetypes', 'value'),
     Input('radio-qa-pivot', 'value'),
     Input('button-qa-refresh', 'n_clicks')])
def update_all(
    selected_proc,
    selected_scan,
    selected_sess,
    selected_proj,
    selected_starttime,
    selected_endtime,
    selected_groupby,
    selected_hidetypes,
    selected_pivot,
    n_clicks
):
    refresh = False

    logger.debug('update_all')

    # Load our data
    # This data will already be merged scans and assessors with
    # a row per scan or assessor
    ctx = dash.callback_context
    if was_triggered(ctx, 'button-qa-refresh'):
        # Refresh data if refresh button clicked
        logger.debug('refresh:clicks={}'.format(n_clicks))
        refresh = True

    logger.debug('loading data')
    hidetypes = (selected_hidetypes == 'HIDE')
    df = load_data(refresh=refresh, hidetypes=hidetypes)

    # Truncate NOTE
    df['NOTE'] = df['NOTE'].str.slice(0, 70)

    # Update lists of possible options for dropdowns (could have changed)
    # make these lists before we filter what to display
    proj = utils.make_options(load_proj_options())
    scan = utils.make_options(load_scan_options(selected_proj))
    sess = utils.make_options(load_sess_options(selected_proj))
    proc = utils.make_options(load_proc_options(selected_proj))

    # Filter data based on dropdown values
    df = data.filter_data(
        df,
        selected_proj,
        selected_proc,
        selected_scan,
        selected_starttime,
        selected_endtime,
        selected_sess)

    #if selected_hidetypes == 'HIDE':
    #    df = data.filter_types(df)

    # Get the qa pivot from the filtered data
    dfp = qa_pivot(df)

    tabs = _get_graph_content(dfp, selected_groupby)

    if selected_pivot == 'proj':
        cols = []
        selected_cols = ['PROJECT']

        dfp = dfp .reset_index()

        if selected_proc:
            cols += selected_proc
            selected_cols += selected_proc

        if selected_scan:
            cols += selected_scan
            selected_cols += selected_scan

        if cols:
            cols = [x for x in cols if x in dfp.columns]

            # aggregrate to most common value (mode)
            dfp = dfp.reset_index().pivot_table(
                index=('PROJECT'),
                values=cols,
                aggfunc=pd.Series.mode)

            if selected_proc:
                for p in selected_proc:
                    if p in dfp.columns:
                        dfp[p] = dfp[p].str.replace('P', '✅')
                        dfp[p] = dfp[p].str.replace('X', '🩷')
                        dfp[p] = dfp[p].str.replace('Q', '🟩')
                        dfp[p] = dfp[p].str.replace('N', '🟡')
                        dfp[p] = dfp[p].str.replace('R', '🔷')
                        dfp[p] = dfp[p].str.replace('F', '❌')

            if selected_scan:
                for s in selected_scan:
                    if s in dfp.columns:
                        dfp[s] = dfp[s].str.replace('P', '✅')
                        dfp[s] = dfp[s].str.replace('X', '🩷')
                        dfp[s] = dfp[s].str.replace('Q', '🟩')
                        dfp[s] = dfp[s].str.replace('N', '🟡')
                        dfp[s] = dfp[s].str.replace('R', '🔷')
                        dfp[s] = dfp[s].str.replace('F', '❌')

        else:
            # No types selected, show sessions concat
            selected_cols += ['SESSIONS']

            dfp = dfp[dfp.SESSTYPE != 'SGP']
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

        # Format records
        #for r in records:
        #    if r['PROJECT'] and 'PROJECTLINK' in r:
        #        _proj = r['PROJECT']
        #        _link = r['PROJECTLINK']
        #        r['PROJECT'] = f'[{_proj}]({_link})'

        # Format columns
        #for i, c in enumerate(columns):
        #    if c['name'] == 'PROJECT':
        #        columns[i]['type'] = 'text'
        #        columns[i]['presentation'] = 'markdown'

    elif selected_pivot == 'subj':
        # row per subject

        cols = []
        selected_cols = ['PROJECT', 'SUBJECT']

        if selected_proc:
            cols += selected_proc
            selected_cols += selected_proc

        if selected_scan:
            cols += selected_scan
            selected_cols += selected_scan

        if cols:
            # Show the selected columns

            # only modify columns with data
            cols = [x for x in cols if x in dfp.columns]

            # aggregrate to most common value (mode)
            # TODO: ignore PETs when determining mode for assessors, how?
            dfp = dfp.reset_index().pivot_table(
                index=('PROJECT', 'SUBJECT'),
                values=cols,
                aggfunc=pd.Series.mode)

            if selected_proc:
                for p in selected_proc:
                    if p in dfp.columns:
                        dfp[p] = dfp[p].str.replace('P', '✅')
                        dfp[p] = dfp[p].str.replace('X', '🩷')
                        dfp[p] = dfp[p].str.replace('Q', '🟩')
                        dfp[p] = dfp[p].str.replace('N', '🟡')
                        dfp[p] = dfp[p].str.replace('R', '🔷')
                        dfp[p] = dfp[p].str.replace('F', '❌')

            if selected_scan:
                for s in selected_scan:
                    if s in dfp.columns:
                        dfp[s] = dfp[s].str.replace('P', '✅')
                        dfp[s] = dfp[s].str.replace('X', '🩷')
                        dfp[s] = dfp[s].str.replace('Q', '🟩')
                        dfp[s] = dfp[s].str.replace('N', '🟡')
                        dfp[s] = dfp[s].str.replace('R', '🔷')
                        dfp[s] = dfp[s].str.replace('F', '❌')
        else:
            dfp = dfp.reset_index()
            dfp = dfp[dfp.SESSTYPE != 'SGP']
            dfp = dfp.sort_values('MODALITY')
            dfp['SESSIONS'] = dfp['MODALITY'].map(MOD2EMO).fillna('?')

            dfp = dfp.pivot_table(
                index=('SUBJECT', 'PROJECT'),
                values='SESSIONS',
                aggfunc=lambda x: ''.join(x))

            # Get the table data
            selected_cols = ['SUBJECT', 'PROJECT', 'SESSIONS']

        # Format as column names and record dictionaries for dash table
        columns = utils.make_columns(selected_cols)
        records = dfp.reset_index().to_dict('records')
    else:
        # Get the table data
        selected_cols = [
            'SESSION', 'SUBJECT', 'PROJECT', 'DATE', 'SESSTYPE', 'SITE']

        if selected_proc:
            selected_cols += selected_proc

        if selected_scan:
            selected_cols += selected_scan

        # TODO: move this to where status is mapped to letters
        if selected_proc:
            for p in selected_proc:
                if p in dfp.columns:
                    dfp[p] = dfp[p].str.replace('P', '✅')
                    dfp[p] = dfp[p].str.replace('X', '🩷')
                    dfp[p] = dfp[p].str.replace('Q', '🟩')
                    dfp[p] = dfp[p].str.replace('N', '🟡')
                    dfp[p] = dfp[p].str.replace('R', '🔷')
                    dfp[p] = dfp[p].str.replace('F', '❌')

        if selected_scan:
            for s in selected_scan:
                if s in dfp.columns:
                    dfp[s] = dfp[s].str.replace('P', '✅')
                    dfp[s] = dfp[s].str.replace('X', '🩷')
                    dfp[s] = dfp[s].str.replace('Q', '🟩')
                    dfp[s] = dfp[s].str.replace('N', '🟡')
                    dfp[s] = dfp[s].str.replace('R', '🔷')
                    dfp[s] = dfp[s].str.replace('F', '❌')

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

        # Format columns
        for i, c in enumerate(columns):
            if c['name'] == 'SESSION':
                columns[i]['type'] = 'text'
                columns[i]['presentation'] = 'markdown'




        # TODO: link each project to redcap/xnat in project view


    # TODO: should we only include data for selected columns here,
    # to reduce amount of data sent?

    # Count how many rows are in the table
    rowcount = '{} rows'.format(len(records))

    # Return table, figure, dropdown options
    logger.debug('update_all:returning data')
    return [proc, scan, sess, proj, records, columns, tabs, rowcount]
