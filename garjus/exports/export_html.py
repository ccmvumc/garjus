"""Creates html export of data with web app"""
import os
import logging
from datetime import datetime
from pathlib import Path
import json

import duckdb
import pandas as pd

from .export_templates import main_html_template, grid_js_template, tab_button_html_template, tab_panel_html_template


logger = logging.getLogger('garjus')


# TODO: show export datetime in html

TABLES = ['assessors', 'scans', 'sessions', 'analyses', 'processors', 'stats']

SUBJ_COLUMNS = ['ID', 'PROJECT', 'GROUP', 'AGE', 'SEX']

SCAN_COLUMNS = ['PROJECT', 'SESSION', 'SCANID', 'SCANTYPE']

SESS_COLUMNS = ['PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'SITE', 'NOTE']

ASSR_COLUMNS = ['ASSR', 'DATE', 'JOBDATE', 'STATUS']

STAT_COLUMNS = ['PROJECT', 'SUBJECT', 'SESSION', 'ASSR', 'PROCTYPE']

ASIS_COLUMNS = ['PROJECT', 'ID', 'NAME', 'STATUS', 'REPORT']

PROC_COLUMNS = [
    'ID', 'PROJECT', 'TYPE', 'FILTER', 'ARGS',
]

TASK_COLUMNS = [
    'ID', 'IDLINK', 'PROJECT', 'STATUS', 'PROCTYPE', 'MEMREQ', 'WALLTIME',
    'TIMEUSED', 'MEMUSED', 'ASSESSOR', 'PROCDATE', 'INPUTLIST', 'VAR2VAL',
    'IMAGEDIR', 'JOBTEMPLATE', 'YAMLFILE', 'YAMLUPLOAD', 'USERINPUTS', 
    'FAILCOUNT', 'USER'
]

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

    analyses = g.analyses(projects=projects, download=False)

    # Create dict of all data
    data['subjects'] = subjects
    data['stats'] = stats
    data['scans'] = scans
    data['assessors'] = assessors
    data['processors'] = processors
    data['analyses'] = analyses

    data['sessions'] = pd.concat([data['scans'], data['assessors']])

    return data


def _grid(label, rowdata, coldefs):
    grid_js = grid_js_template
    grid_js = grid_js.replace('ID', label)
    grid_js = grid_js.replace('ROWS', json.dumps(rowdata, default=str))
    grid_js = grid_js.replace('COLUMNS', json.dumps(coldefs, default=str))

    return grid_js


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

        tab_button = tab_button_html_template
        tab_button = tab_button.replace('ID', tab_label)
        tab_button = tab_button.replace('LABEL', tab_label)

        tab_panel = tab_panel_html_template
        tab_panel = tab_panel.replace('ID', tab_label)
        tab_panel = tab_panel.replace('LABEL', tab_label)
        tab_panel = tab_panel.replace('PANEL', tab_data)

        buttons.append(tab_button)
        panels.append(tab_panel)

    return buttons, panels


def _coldefs(df):
    return [{'field': c, 'headerName': c} for c in df.columns]


def _records(df):
    return df.to_dict('records')


def _get_grids(data):
    grids = []

    # Assessors
    _label = 'assessors'
    _data = _records(data['assessors'])
    _defs = _coldefs(data['assessors'])
    _defs = _hide_columns(_defs, ASSR_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Scans
    _label = 'scans'
    _data = _records(data['scans'])
    _defs = _coldefs(data['scans'])
    _defs = _hide_columns(_defs, SCAN_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Sessions
    _label = 'sessions'
    _data = _records(data['sessions'])
    _defs = _coldefs(data['sessions'])
    _defs = _hide_columns(_defs, SESS_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Stats
    _label = 'stats'
    _data = _records(data['stats'])
    _defs = _coldefs(data['stats'])
    _defs = _hide_columns(_defs, STAT_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Analyses
    _label = 'analyses'
    _data = _records(data['analyses'])
    _defs = _coldefs(data['analyses'])
    _defs = _hide_columns(_defs, ASIS_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    # Processors
    _label = 'processors'
    _data = _records(data['processors'])
    _defs = _coldefs(data['processors'])
    _defs = _hide_columns(_defs, PROC_COLUMNS)
    grids.append(_grid(_label, _data, _defs))

    return grids


def _to_html(data):
    html_text = ''
    home_panel = ''
    sessions_panel = ''
    assessors_panel = ''
    scans_panel = ''
    processors_panel = ''
    analyses_panel = ''
    stats_panel = ''

    tabs = [
        {'label': 'home', 'panel': home_panel},
        {'label': 'sessions', 'panel': sessions_panel},
        {'label': 'assessors', 'panel': assessors_panel},
        {'label': 'scans', 'panel': scans_panel},
        {'label': 'processors', 'panel': processors_panel},
        {'label': 'analyses', 'panel': analyses_panel},
        {'label': 'stats', 'panel': stats_panel},
    ]

    tab_buttons, tab_panels = _get_tabs(tabs)

    grids = _get_grids(data)

    # Insert tabs pieces into webpage
    html_text = main_html_template
    html_text = html_text.replace('TABBUTTONS', ''.join(tab_buttons))
    html_text = html_text.replace('TABPANELS', ''.join(tab_panels))
    html_text = html_text.replace('GRIDJS', ''.join(grids))

    return html_text


def _save_data(data, filename):

    con = duckdb.connect(filename)

    for t, df in data.items():
        print(f'saving:{t}')
        con.execute(f'CREATE TABLE {t} AS SELECT * FROM df')

    print('DONE!')


def _duck_data(filename):
    data = {}

    con = duckdb.connect(filename)

    for t in TABLES:
        data[t] = con.sql(f"SELECT * FROM {t}").df()

    return data


def export_html(
    g,
    filename,
    projects, 
    proctypes=None,
    sesstypes=None,
    sessions=None
):  
    duck_file = f'{filename}.duckdb'

    # Load data as dataframes
    if os.path.exists(duck_file):
        print(f'loading duckfile:{duck_file}')
        data = _duck_data(duck_file)
    else:
        data = _load_data(
            g,
            projects,
            proctypes=proctypes,
            sesstypes=sesstypes,
            sessions=sessions
        )
        print(f'saving duckdb:{duck_file}')
        _save_data(data, duck_file)

    # Get html from ag grid data
    html_text = _to_html(data)

    # Write html to file
    print(f'saving html:{filename}:{projects=}')
    logger.debug(f'saving to file:{filename}')
    _write_html(html_text, filename)
