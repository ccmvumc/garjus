"""Creates html export of data with web app"""
# The default dashboard has tabs for: stats, qa, analyses, processors
#
# stats
# Gives access to tabular outputs from processor pipelines. 
# User selects one or more projects followed by one or more processing types. 
# The tables loads a row for each assessor with 
# columns for each stat as well as the project/subject/session/.
# 
# qa
# The qa grid gives access to all scan and assessor information. 
# These can be pivoted by session/subject/project to summarize at that level.
# Initially, the SESSIONS pivot is loaded which shows a row per session.
#
# analyses
#
# processors
# 
# TBD: activity, issues, queue, reports
#
# To pivot on QA and stats, we rebuild the data on the fly
# stats pivot by session/subject, preload data row per assessor
# qa pivot by session/subject/project, preload scans and assessors
#
# Other Features:
# dark/light mode toggle
#
# TODO: markdown links
# TODO: filter radio buttons
# TODO: columns selectors?
# TODO: date filter?
# TODO: autofilter button that hides scan types not used in any assessors?
# TODO: graphsgraphsgraphs
# TODO: home grid
import os
import logging
from datetime import datetime
from pathlib import Path
import json
from importlib import resources

import duckdb
import pandas as pd
import numpy as np

from .export_templates import main_html_template, grid_js_template
from .export_templates import tab_button_active_html_template, tab_panel_active_html_template
from .export_templates import tab_button_html_template, tab_panel_html_template
from .export_templates import tab_button_active_html_template, tab_panel_active_html_template
from .export_templates import tab_button_html_template, tab_panel_html_template
from .export_templates import dropdown_js_template, dropdown_html_template, dropdown_option_html_template
from .export_templates import csv_button_html_template, badge_html_template
from .export_templates import pivot_bar_html_template, pivot_button_html_template
from .export_templates import pivot_button_active_html_template
from .export_templates import stats_data_js_template, qa_data_js_template, data_dict_js_template
from .export_templates import graph_html_template, filters_row_html_template


logger = logging.getLogger('garjus')


# Names of tables saved to .duckdb database file
TABLES = ['assessors', 'scans', 'analyses', 'processors', 'stats']

SUBJ_COLUMNS = ['ID', 'PROJECT', 'GROUP', 'AGE', 'SEX']

QA_COLUMNS = ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'STATUS', 'DATE', 'SITE', 'NOTE']

STAT_COLUMNS = ['ASSR', 'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'SITE', 'DATE', 'PROCTYPE']

ANALYSES_COLUMNS = ['PROJECT', 'ID', 'NAME', 'STATUS', 'REPORT']

PROCESSORS_COLUMNS = ['ID', 'PROJECT', 'TYPE', 'FILTER', 'ARGS']

TASK_COLUMNS = ['ID', 'PROJECT', 'STATUS', 'PROCTYPE', 'TIMEUSED', 'MEMUSED']


def _load_data(g, projects, proctypes=None, sesstypes=None, sessions=None):
    data  = {}
    stats = pd.DataFrame()
    scans = pd.DataFrame()
    subjects = pd.DataFrame()
    assessors = pd.DataFrame()
    processors = pd.DataFrame()
    analyses = pd.DataFrame()

    if not isinstance(projects, list):
        projects = projects.split(',')

    if proctypes is not None and not isinstance(proctypes, list):
        proctypes = proctypes.split(',')

    if sesstypes is not None and not isinstance(sesstypes, list):
        sesstypes = sesstypes.split(',')

    if sessions is not None and not isinstance(sessions, list):
        sessions = sessions.split(',')

    for p in sorted(projects):
        # Load project subjects
        psubjects = g.subjects(p).reset_index()

        try:
            # Load project stats
            pstats = g.stats(p, proctypes=proctypes, sesstypes=sesstypes)

            # Check for empty
            if len(pstats) == 0:
                logger.info(f'no stats for project:{p}')
                continue
        except Exception:
            logger.info(f'failed to load stats, check REDCap connection')
            continue

        # Append to total
        subjects = pd.concat([subjects, psubjects])
        stats = pd.concat([stats, pstats])

    # Only include specifc subset of columns
    if len(subjects) > 0:
        subjects = subjects[SUBJ_COLUMNS]

        # Only stats for subjects in subjects
        stats = stats[stats.SUBJECT.isin(subjects.ID.unique())]

        # Only subjects with stats
        subjects = subjects[subjects.ID.isin(stats.SUBJECT.unique())]

        if 'SITE' in subjects.columns:
            subjects['SITE'] = subjects['SITE'].replace({'PITT': 'UPMC'})
    else:
        subjects = pd.DataFrame(SUBJ_COLUMNS)

    # Make PITT be UPMC
    if len(stats) > 0:
        stats['SITE'] = stats['SITE'].replace({'PITT': 'UPMC'})
    else:
        stats = pd.DataFrame()

    scans = g.scans(projects=projects, sesstypes=sesstypes)

    assessors = g.assessors(projects=projects, sesstypes=sesstypes)

    processors = g.processing_protocols()
    processors = processors[processors.PROJECT.isin(projects)]

    analyses = g.analyses(projects=projects, download=False)

    # Create dict of all data
    data['subjects'] = subjects
    data['stats'] = stats
    data['scans'] = scans
    data['assessors'] = assessors
    data['processors'] = processors
    data['analyses'] = analyses

    return data


def _grid(label, rowdata, coldefs):
    grid_js = grid_js_template
    grid_js = grid_js.replace('ID', label)
    grid_js = grid_js.replace('ROWS', json.dumps(rowdata, default=str))
    grid_js = grid_js.replace('COLUMNS', json.dumps(coldefs, default=str))

    return grid_js


def _dropdown_html(ident, label, options):

    dropdown_options = [_dropdown_option(x) for x in options]
    dropdown_html = dropdown_html_template
    dropdown_html = dropdown_html.replace('ID', ident)
    dropdown_html = dropdown_html.replace('LABEL', label)
    dropdown_html = dropdown_html.replace('OPTIONS', ''.join(dropdown_options))

    return dropdown_html


def _dropdown_js(label):
    dropdown_js = dropdown_js_template
    dropdown_js = dropdown_js.replace('ID', label)

    return dropdown_js


def _dropdown_option(label):
    return dropdown_option_html_template.replace('LABEL', label).replace('VALUE', label)


def _badge(ident):
    return badge_html_template.replace('ID', ident)


def _graph(ident):
    return graph_html_template.replace('ID', ident)


def _get_home_tab():
    tab_button = tab_button_html_template
    tab_panel = tab_panel_html_template
    label = 'home'

    tab_button = tab_button.replace('ID', label)
    tab_button = tab_button.replace('LABEL', label)

    tab_panel = tab_panel.replace('ID', label)
    tab_panel = tab_panel.replace('LABEL', label)

    return tab_button, tab_panel


def _hide_columns(column_defs, show_columns):
    for i, c in enumerate(column_defs):
        if c['field'] not in show_columns:
            c['hide'] = True

    return column_defs


def _write_html(html_text, filename):
    _path = Path(filename)
    _path.write_text(html_text, encoding="utf-8")


def _get_tabs(tabs):
    buttons = []
    panels = []

    for t in tabs:
        tab_label = t['label']
        tab_data = t['panel']
        tab_active = t['active']

        if tab_active:
            tab_button = tab_button_active_html_template
            tab_panel = tab_panel_active_html_template
        else:
            tab_button = tab_button_html_template
            tab_panel = tab_panel_html_template
    
        tab_button = tab_button.replace('ID', tab_label)
        tab_button = tab_button.replace('LABEL', tab_label)

        tab_panel = tab_panel.replace('ID', tab_label)
        tab_panel = tab_panel.replace('LABEL', tab_label)
        tab_panel = tab_panel.replace('PANEL', tab_data)

        buttons.append(tab_button)
        panels.append(tab_panel)

    return buttons, panels


def _coldefs(df):
    return [{'field': c, 'headerName': c} for c in df.columns]


def _records(df):
    records = df.to_dict('records')
    records = [{k:v for k,v in r.items() if v} for r in records]
    return records


def _get_data_dict(data):
    proc2stats = data['proc2stats']
    proj2procs = data['proj2procs']
    proj2sesstypes = data['proj2sesstypes']
    proj2scantypes = data['proj2scantypes']

    data_dict = data_dict_js_template
    data_dict = data_dict.replace('PROC2STATS', json.dumps(proc2stats, default=str))
    data_dict = data_dict.replace('PROJ2PROCS', json.dumps(proj2procs, default=str))
    data_dict = data_dict.replace('PROJ2SESSTYPES', json.dumps(proj2sesstypes, default=str))
    data_dict = data_dict.replace('PROJ2SCANTYPES', json.dumps(proj2scantypes, default=str))

    return data_dict


def _get_qa_data(data):
    row_data = _records(data['qa'])
    col_defs = _coldefs(data['qa'])
    col_defs = _hide_columns(col_defs, QA_COLUMNS)

    qa_data = qa_data_js_template
    qa_data = qa_data.replace('ROWS', json.dumps(row_data, default=str))
    qa_data = qa_data.replace('COLUMNS', json.dumps(col_defs, default=str))

    return qa_data


def _get_stats_data(data):
    row_data = _records(data['stats'])
    col_defs = _coldefs(data['stats'])
    stats_data = stats_data_js_template
    stats_data = stats_data.replace('ROWS', json.dumps(row_data, default=str))
    stats_data = stats_data.replace('COLUMNS', json.dumps(col_defs, default=str))

    return stats_data


def _get_grids(data):
    grids = []

    # QA initialize with no data loaded
    _label = 'qa'
    _data = []
    _defs = _coldefs(data['qa'])
    #_defs = _hide_columns(_defs, SESS_COLUMNS)
    _defs = [];
    grids.append(_grid(_label, _data, _defs))

    # Stats initialize with no data loaded
    _label = 'stats'
    _data = []
    _defs = _coldefs(data['stats'])
    _defs = _hide_columns(_defs, STAT_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Analyses
    _label = 'analyses'
    _data = _records(data['analyses'])
    _defs = _coldefs(data['analyses'])
    _defs = _hide_columns(_defs, ANALYSES_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Processors
    _label = 'processors'
    _data = _records(data['processors'])
    _defs = _coldefs(data['processors'])
    _defs = _hide_columns(_defs, PROCESSORS_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    return grids


def _get_buttons(data):
    buttons = []

    # Add pre javascript
    buttons.append(_dashboard_pre_js())

    # Stats dropdowns
    buttons.append(_dropdown_js('stats_projects'))
    buttons.append(_dropdown_js('stats_sesstypes'))
    buttons.append(_dropdown_js('stats_proctypes'))
    buttons.append(_dropdown_js('stats_measures'))
    buttons.append(_dropdown_js('stats_xvariable'))

    # QA dropdowns
    buttons.append(_dropdown_js('qa_projects'))
    buttons.append(_dropdown_js('qa_proctypes'))
    buttons.append(_dropdown_js('qa_scantypes'))
    buttons.append(_dropdown_js('qa_sesstypes'))

    # Analyses dropdowns
    buttons.append(_dropdown_js('analyses_projects'))
    buttons.append(_dropdown_js('analyses_invest'))
    buttons.append(_dropdown_js('analyses_status'))

    # Processors dropdowns
    buttons.append(_dropdown_js('processors_projects'))

    # Add post javascript
    buttons.append(_dashboard_post_js())

    return buttons


def _dashboard_pre_js():
    return resources.read_text('garjus.dashboard', 'assets/dashboard_pre.js')


def _dashboard_post_js():
    return resources.read_text('garjus.dashboard', 'assets/dashboard_post.js')


def _home_panel(data):
    # proc grid
    panel = 'TODO: processing grid'
    return panel


def _filters_row(filters, graphs):
    filters_row = filters_row_html_template
    filters_row = filters_row.replace('FILTERS', filters)
    filters_row = filters_row.replace('GRAPHS', graphs)
    return filters_row


def _stats_panel(data):
    # Tab buttons for pivot select: Assessors or Sessions or Subjects
    panel = ''

    # Export button
    panel += _csv_button('stats')

    # Filters and Graph in next row
    _projects = sorted(list(data['assessors'].PROJECT.unique()))
    _proctypes = sorted(list(data['assessors'].PROCTYPE.unique()))
    _sesstypes = sorted(list(data['assessors'].SESSTYPE.unique()))
    _measures = data['stats'].columns
    _dropdowns = ''.join([
        _dropdown_html('stats_projects', 'Projects', _projects),
        _dropdown_html('stats_sesstypes', 'Session Types', _sesstypes ),
        _dropdown_html('stats_proctypes', 'Processing Types', _proctypes),
        _dropdown_html('stats_measures', 'Measures', _measures),
        _dropdown_html('stats_xvariable', 'x-variable', _measures)])
    _graphs = ''.join([_graph('stats')])
    panel += _filters_row(_dropdowns, _graphs)

    # Row count badge
    panel += _badge('stats_rowcount')

    # Pivot buttons
    panel += _stats_pivot_buttons()

    return panel



def _qa_panel(data):
    panel = ''
    # date selector Start Date to End Date
    # radio button for autofilter
    # radio button for graphs
    # radio button for demographics
    # radio buttons: MR PET EEG
    # radio buttons: emojis for statuses
    # buttons for pivot select

    # Export as csv button
    panel += _csv_button('qa')

    # Filters
    _projects = sorted(list(data['scans'].PROJECT.unique()))
    _proctypes = sorted(list(data['assessors'].PROCTYPE.unique()))
    _scantypes = sorted(list(data['scans'].SCANTYPE.unique()))
    _sesstypes = sorted(list(data['scans'].SESSTYPE.unique()))
    _dropdowns = ''.join([
        _dropdown_html('qa_projects', 'Projects', _projects),
        _dropdown_html('qa_sesstypes', 'Session Types', _sesstypes),
        _dropdown_html('qa_scantypes', 'Scan Types', _scantypes),
        _dropdown_html('qa_proctypes', 'Processing Types', _proctypes)
    ])

    # Graph
    panel += _filters_row(_dropdowns, _graph('qa'))

    # Row count badge
    panel += _badge('qa_rowcount')

    # Pivot buttons
    panel += _qa_pivot_buttons()

    return panel


def _qa_pivot_buttons():
    buttons = []

    buttons.append(_pivot_button('qa_scans', 'SCANS'))
    buttons.append(_pivot_button('qa_assessors', 'ASSESSORS'))
    buttons.append(_pivot_button_active('qa_sessions', 'SESSIONS'))
    buttons.append(_pivot_button('qa_subjects', 'SUBJECTS'))
    buttons.append(_pivot_button('qa_projects', 'PROJECTS'))

    return pivot_bar_html_template.replace('PIVOTBUTTONS', ''.join(buttons))


def _stats_pivot_buttons():
    buttons = []

    buttons.append(_pivot_button_active('stats_assessors', 'ASSESSORS'))
    buttons.append(_pivot_button('stats_sessions', 'SESSIONS'))
    buttons.append(_pivot_button('stats_subjects', 'SUBJECTS'))

    return pivot_bar_html_template.replace('PIVOTBUTTONS', ''.join(buttons))


def _pivot_button(ident, label):
    return pivot_button_html_template.replace('ID', ident).replace('LABEL', label)


def _pivot_button_active(ident, label):
    return pivot_button_active_html_template.replace('ID', ident).replace('LABEL', label)



def _csv_button(ident):
    return csv_button_html_template.replace('ID', ident)


def _processors_panel(data):
    panel = ''

    panel += _csv_button('processors')

    _projects = sorted(data['assessors'].PROJECT.unique())
    panel += _dropdown_html('processors_projects', 'Projects', _projects)

    # Row count badge
    panel += _badge('processors_rowcount')

    return panel


def _analyses_panel(data):
    panel = ''

    panel += _csv_button('analyses')

    # Filters
    _projects = sorted(list(data['assessors'].PROJECT.unique()))
    _investigators = sorted(list(data['analyses'].INVESTIGATOR.unique()))
    _statuses = sorted(list(data['analyses'].STATUS.unique()))
    panel += _dropdown_html('analyses_projects', 'Projects', _projects)
    panel += _dropdown_html('analyses_invest', 'Investigator', _investigators)
    panel += _dropdown_html('analyses_status', 'Status', _statuses)

    # Row count badge
    panel += _badge('analyses_rowcount')

    return panel


def _to_html(data):
    html_text = ''

    tabs = [
        {'label': 'qa', 'panel': _qa_panel(data),  'active': True},
        {'label': 'stats', 'panel': _stats_panel(data), 'active': False},
        #{'label': 'home', 'panel': _home_panel(data)},
        {'label': 'processors', 'panel': _processors_panel(data),  'active': False},
        {'label': 'analyses', 'panel': _analyses_panel(data),  'active': False},
    ]

    tab_buttons, tab_panels = _get_tabs(tabs)

    qa_data = _get_qa_data(data)

    stats_data = _get_stats_data(data)

    data_dict = _get_data_dict(data)

    grids = ''.join(_get_grids(data))

    buttons = ''.join(_get_buttons(data))

    # Insert tabs pieces into webpage
    html_text = main_html_template
    html_text = html_text.replace('TIMESTAMP', data['timestamp'])
    html_text = html_text.replace('TABBUTTONS', ''.join(tab_buttons))
    html_text = html_text.replace('TABPANELS', ''.join(tab_panels))
    html_text = html_text.replace('BUTTONJS', buttons)
    html_text = html_text.replace('GRIDJS', qa_data + stats_data + data_dict + grids)

    return html_text


def _save_data(data, filename):

    con = duckdb.connect(filename)

    for t, df in data.items():
        logger.info(f'saving:{t}')
        con.execute(f'CREATE TABLE {t} AS SELECT * FROM df')

    logger.info('DONE!')


def _duck_data(filename):
    data = {}

    con = duckdb.connect(filename)

    for t in TABLES:
        data[t] = con.sql(f"SELECT * FROM {t}").df()
        data[t] = data[t].fillna('')

    return data


def _map_proc2stats(stats):
    proc2stats = {}

    for proc in stats.PROCTYPE.unique():
        # Get columns with values for this proctype, no blanks
        df = stats[stats.PROCTYPE == proc]
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.dropna(axis=1, how='all')
        proc2stats[proc] = [x for x in list(df.columns) if x not in STAT_COLUMNS]

    return proc2stats


def _map_proj2procs(stats):
    proj2procs = {}

    for proj in stats.PROJECT.unique():
        # Get columns with values for this project
        df = stats[stats.PROJECT == proj]
        proj2procs[proj] = sorted(list(df.PROCTYPE.unique()))

    return proj2procs


def _map_proj2sesstypes(stats):
    proj2sesstypes = {}

    for proj in stats.PROJECT.unique():
        # Get columns with values for this project
        df = stats[stats.PROJECT == proj]
        proj2sesstypes[proj] = sorted(list(df.SESSTYPE.unique()))

    return proj2sesstypes


def _map_proj2scantypes(qa):
    proj2scantypes = {}

    for proj in qa.PROJECT.unique():
        # Get columns with values for this project
        df = qa[qa.PROJECT == proj]
        proj2scantypes[proj] = list(df.SCANTYPE.unique())

    return proj2scantypes



def export_html(
    g,
    filename,
    projects, 
    proctypes=None,
    sesstypes=None,
    sessions=None
):
    duck_file = f'{filename}.duckdb'

    # Load data from primary sources
    if not os.path.exists(duck_file):
        logger.info(f'exporting data')
        data = _load_data(
            g,
            projects,
            proctypes=proctypes,
            sesstypes=sesstypes,
            sessions=sessions
        )
        logger.info(f'saving duckdb:{duck_file}')
        _save_data(data, duck_file)

    logger.info(f'loading duckfile:{duck_file}')
    data = _duck_data(duck_file)

    data['timestamp'] = datetime.now().strftime("%I:%M:%S %p %Y-%m-%d ")

    data['qa'] = pd.concat([data['scans'], data['assessors']]).fillna(np.nan)

    data['stats'] = data['stats'].replace('', np.nan).dropna(subset=['SESSTYPE'])

    data['stats'] = data['stats'].replace(r'^\s*$', np.nan, regex=True).replace(np.nan, '')

    data['qa'].DATE = pd.to_datetime(data['qa'].DATE).dt.date

    data['stats'].DATE = pd.to_datetime(data['stats'].DATE).dt.date

    data['qa'].SITE = data['qa'].SITE.replace({'PITT': 'UPMC'})

    # Add processing type to list stats mapping
    data['proc2stats'] = _map_proc2stats(data['stats'])

    # Get map of projects to proc types
    data['proj2procs'] = _map_proj2procs(data['stats'])

    # Get map of projects to sess types
    data['proj2sesstypes'] = _map_proj2sesstypes(data['stats'])

    # Get map of projects to scan types
    data['proj2scantypes'] = _map_proj2scantypes(data['qa'])

    # Get html from ag grid data
    html_text = _to_html(data)

    # Write html to file
    logger.info(f'saving html:{filename}:{projects=}')
    _write_html(html_text, filename)
