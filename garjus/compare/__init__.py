"""Garjus Double Data Entry Comparison."""
from datetime import datetime
import tempfile
import logging
import os

from .dataentry_compare import run_compare


logger = logging.getLogger('garjus.compare')


def update(garjus, projects=None):
    """Update project progress."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating double entry compare:{p}')
            update_project(garjus, p)


def update_project(garjus, project):
    """Update project double entry."""
    double_reports = garjus.double_reports([project])

    # what time is it? use this for naming
    now = datetime.now()

    # determine current month and year to get current monthly repot id
    cur_double = now.strftime("%B%Y")

    # check that each project has report for current month
    has_cur = any(d.get('double_name') == cur_double for d in double_reports)
    if not has_cur:
        logger.debug(f'making new double record:{project}:{cur_double}')
        make_double(garjus, project, cur_double, now)
    else:
        logger.debug(f'double entry record exists:{project}:{cur_double}')


def make_double(garjus, project, cur_double, now):
    # Get the projects to compare
    proj_primary = garjus.primary(project)
    proj_secondary = garjus.secondary(project)

    if not proj_primary:
        logger.debug(f'cannot compare, primary REDCap not set:{project}')
        return

    if not proj_secondary:
        logger.debug(f'cannot run, secondary REDCap not set:{project}')
        return

    """Make double entry comparison report."""
    with tempfile.TemporaryDirectory() as outdir:
        logger.debug(f'created temporary directory:{outdir}')

        fnow = now.strftime("%Y-%m-%d_%H_%M_%S")
        pdf_file = f'{outdir}/{project}_report_{fnow}.pdf'
        excel_file = f'{outdir}/{project}_results_{fnow}.xlsx'

        logger.debug(f'making report:{pdf_file}')
        make_double_report(proj_primary, proj_secondary, pdf_file, excel_file)

        if not os.path.isfile(pdf_file):
            logger.debug(f'no results')
        else:
            logger.debug(f'uploading results')
            garjus.add_double(project, cur_double, now, pdf_file, excel_file)


def make_double_report(proj_primary, proj_secondary, pdf_file, excel_file):
    # Run it
    p1 = proj_primary.export_project_info().get('project_title')
    p2 = proj_secondary.export_project_info().get('project_title')
    logger.debug(f'compare {p1} to {p2}')
    run_compare(proj_primary, proj_secondary, pdf_file, excel_file)
