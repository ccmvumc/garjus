"""

progress reports and exports.

update will create any missing

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


def make_stats_csv(
    garjus, projects, proctypes, sesstypes, csvname, persubject=False):
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
        stats = garjus.stats(
            p, proctypes=proctypes, sesstypes=sesstypes, persubject=persubject)
        df = pd.concat([df, stats])

    # Save file for this type
    logger.info(f'saving csv:{csvname}')
    df.to_csv(csvname, index=False)
