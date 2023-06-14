"""

progress reports and exports.

update will creat any missing

"""

from datetime import datetime
import tempfile
import logging

import pandas as pd

from .report import make_project_report


logger = logging.getLogger('garjus.progress')


def update(garjus, projects=None):
    """Update project progress."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating progress:{p}')
            update_project(garjus, p)


def update_project(garjus, project):
    """Update project progress."""
    progs = garjus.progress_reports(projects=[project])

    # what time is it? use this for naming
    now = datetime.now()

    # determine current month and year to get current monthly repot id
    cur_progress = now.strftime("%B%Y")

    # check that each project has report for current month with PDF and zip
    has_cur = any(d.get('progress_name') == cur_progress for d in progs)
    if not has_cur:
        logger.debug(f'making new progress record:{project}:{cur_progress}')
        make_progress(garjus, project, cur_progress, now)
    else:
        logger.debug(f'progress record exists:{project}:{cur_progress}')


def make_progress(garjus, project, cur_progress, now):
    with tempfile.TemporaryDirectory() as outdir:
        fnow = now.strftime("%Y-%m-%d_%H_%M_%S")
        pdf_file = f'{outdir}/{project}_report_{fnow}.pdf'
        zip_file =  f'{outdir}/{project}_data_{fnow}.zip'

        make_project_report(garjus, project, pdf_file, zip_file)
        garjus.add_progress(project, cur_progress, now, pdf_file, zip_file)

def subject_pivot(df):
    # Pivot to one row per subject
    level_cols = ['SESSTYPE', 'PROCTYPE']
    stat_cols = []
    index_cols = ['PROJECT', 'SUBJECT', 'SITE']

    # Drop any duplicates found
    df = df.drop_duplicates()

    # And duplicate proctype for session
    df = df.drop_duplicates(
        subset=['SUBJECT', 'SESSTYPE', 'PROCTYPE'],
        keep='last')

    df = df.drop(columns=['ASSR', 'SESSION', 'DATE'])

    stat_cols = [x for x in df.columns if (x not in index_cols and x not in level_cols)]

    # Make the pivot table based on _index, _cols, _vars
    dfp = df.pivot(index=index_cols, columns=level_cols, values=stat_cols)

    if len(df.SESSTYPE.unique()) > 1:
        # Concatenate column levels to get one level with delimiter
        dfp.columns = [f'{c[1]}_{c[0]}' for c in dfp.columns.values]
    else:
        dfp.columns = [c[0] for c in dfp.columns.values]

    # Clear the index so all columns are named
    dfp = dfp.dropna(axis=1, how='all')
    dfp = dfp.reset_index()

    return dfp


def make_stats_csv(garjus, projects, proctypes, sesstypes, csvname, persubject=False):
    """"Make the file."""
    df = pd.DataFrame()

    if not isinstance(projects, list):
        projects = projects.split(',')

    if proctypes is not None and not isinstance(proctypes, list):
        proctypes = proctypes.split(',')

    if sesstypes is not None and not isinstance(sesstypes, list):
        sesstypes = sesstypes.split(',')

    for p in sorted(projects):
        # Load stats
        stats = garjus.stats(p, proctypes=proctypes, sesstypes=sesstypes)
        df = pd.concat([df, stats])

    if persubject:
        logger.debug(f'pivot to row per subject')

        # Pivot to row per subject with sesstype prefix when multiple types
        df = subject_pivot(df)

    # Save file for this type
    logger.info(f'saving csv:{csvname}')
    df.to_csv(csvname, index=False)
