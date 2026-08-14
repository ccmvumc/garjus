"""Creates html export of data with web app"""
import logging
from datetime import datetime
from pathlib import Path
import json

import pandas as pd

from .export_templates import main_html_template, grid_js_template, tab_button_html_template, tab_panel_html_template


logger = logging.getLogger('garjus')

# TODO: add export time to html

SUBJ_COLUMNS = ['ID', 'PROJECT', 'GROUP', 'AGE', 'SEX']

SCAN_COLUMNS = ['PROJECT',  'SUBJECT', 'SESSION', 'SCANTYPE']

ASSR_COLUMNS = ['ASSR', 'PROJECT', 'SUBJECT', 'SESSION', 'SESSTYPE', 'DATE', 'SITE', 'NOTE']

STAT_COLUMNS = []

def _load_data(g, projects, proctypes=None, sesstypes=None, sessions=None):
    data  = {}
    stats = pd.DataFrame()
    scans = pd.DataFrame()
    subjects = pd.DataFrame()
    assessors = pd.DataFrame()

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
            print(f'{p}:{proctypes=}:{sesstypes=}')
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

    # Filter duplicate GUID to handle same subject in multiple projects
    #if 'GUID' in subjects.columns:
    #    subjects = subjects[(subjects['GUID'] == '') | (subjects['GUID'].isna()) | ~subjects.duplicated(subset='GUID')]

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

    # Create dict of all data
    data['subjects'] = subjects
    data['stats'] = stats
    data['scans'] = scans
    data['assessors'] = assessors

    return data


def _init_grid(label, df):
    return {
        'id': label,
        'rowData': df.to_dict('records'),
        'columnDefs': [{'field': c, 'headerName': c} for c in df.columns],
    }


def _get_tab(label, grid_data):
    # Initialize button for selecting tab and panel for holding data grid
    tab_button = tab_button_html_template
    tab_panel = tab_panel_html_template
    tab_js = grid_js_template

    # Insert data into javascript
    tab_js = tab_js.replace('ID', label)
    tab_js = tab_js.replace('ROWS', json.dumps(grid_data['rowData'], default=str))
    tab_js = tab_js.replace('COLUMNS', json.dumps(grid_data['columnDefs'], default=str))

    # Label panel
    tab_panel = tab_panel.replace('ID', label)
    tab_panel = tab_panel.replace('LABEL', label)

    # Label button
    tab_button = tab_button.replace('ID', label)
    tab_button = tab_button.replace('LABEL', label)
    tab_button = tab_button.replace('COUNT', str(len(grid_data['rowData'])))

    return tab_js, tab_button, tab_panel


def _get_html(data):
    html_text = main_html_template
    tab_buttons = []
    tab_panels = []
    tab_js = []

    # Build scan tab
    _grid = _init_grid('scans', data['scans'])

    # Hide non-core columns
    for i, c in enumerate(_grid['columnDefs']):
        if c['field'] not in SCAN_COLUMNS:
            print(f'hiding column:{i}:{c}')
            c['hide'] = True

    _js, _button, _panel = _get_tab('scans', _grid)
    tab_js.append(_js)
    tab_buttons.append(_button)
    tab_panels.append(_panel)

    # Build assessor tab
    _grid = _init_grid('assessors', data['assessors'])

    # Hide non-core columns
    for i, c in enumerate(_grid['columnDefs']):
        if c['field'] not in ASSR_COLUMNS:
            print(f'hiding column:{i}:{c}')
            c['hide'] = True

    _js, _button, _panel = _get_tab('assessors', _grid)
    tab_js.append(_js)
    tab_buttons.append(_button)
    tab_panels.append(_panel)

    # Build stats tab
    _grid = _init_grid('stats', data['stats'])

    # Hide non-core columns
    for i, c in enumerate(_grid['columnDefs']):
        if c['field'] not in STAT_COLUMNS:
            print(f'hiding column:{i}:{c}')
            c['hide'] = True

    _js, _button, _panel = _get_tab('stats', _grid)
    tab_js.append(_js)
    tab_buttons.append(_button)
    tab_panels.append(_panel)

    # Insert tabs pieces into webpage
    html_text = html_text.replace('TABBUTTONS', ''.join(tab_buttons))
    html_text = html_text.replace('TABPANELS', ''.join(tab_panels))
    html_text = html_text.replace('TABJS', ''.join(tab_js))

    return html_text


def _write_html(html_text, filename):
    _path = Path(filename)
    _path.write_text(html_text, encoding="utf-8")


def export_html(
    g,
    filename,
    projects, 
    proctypes=None,
    sesstypes=None,
    sessions=None
):
    print(f'saving html:{filename}:{projects=}')
    logger.debug(f'saving to file:{filename}')

    # Load data as dataframes
    data = _load_data(
        g,
        projects,
        proctypes=proctypes,
        sesstypes=sesstypes,
        sessions=sessions
    )

    # Get html from ag grid data
    html_text = _get_html(data)

    # Write html to file
    _write_html(html_text, filename)

    print(html_text)
