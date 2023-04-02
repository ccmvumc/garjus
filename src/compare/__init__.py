"""Garjus Double Data Entry Comparison."""
from datetime import datetime
import tempfile
import logging

from .dataentry_compare import run_compare


def update(garjus, projects=None):
    """Update project progress."""
    for p in (projects or garjus.projects()):
        if p in projects:
            logging.info(f'updating progress:{p}')
            update_project(garjus, p)


def update_project(garjus, project):
    """Update project double entry."""
    double_reports = garjus.double(projects=[project])

    # what time is it? use this for naming
    now = datetime.now()

    # determine curxrent month and year to get current monthly repot id
    cur_double = now.strftime("%B%Y")

    # check that each project has report for current month with PDF and zip
    has_cur = any(d.get('double_name') == cur_double for d in double_reports)
    if not has_cur:
        logging.debug(f'making new double record:{project}:{cur_double}')
        make_double(garjus, project, cur_double, now)
    else:
        logging.debug(f'double entry record exists:{project}:{cur_double}')


def make_double(garjus, project, cur_double, now):
    """Make double entry comparison report."""
    with tempfile.TemporaryDirectory() as outdir:
        logging.info(f'created temporary directory:{outdir}')

        fnow = now.strftime("%Y-%m-%d_%H_%M_%S")
        pdf_file = f'{outdir}/{project}_report_{fnow}.pdf'
        excel_file = f'{outdir}/{project}_stats_{fnow}.zip'

        logging.info(f'making report:{pdf_file}')
        make_project_report(garjus, project, pdf_file, zip_file)

        logging.info(f'uploading results')
        garjus.add_double(project, cur_double, now, pdf_file, excel_file)


def make_double_report(garjus, project, pdf_file, excel_file):
	# Get the projects to compare
    proj_primary = garjus.primary(project)
    proj_secondary = garjus.secondary(project)

    if not proj_primary:
        logging.warning(f'cannot compare, primary REDCap not set:{proj_name}')
        return

    if not proj_secondary:
        logging.warning(f'cannot run, secondary REDCap not set:{proj_name}')
        return

    # Run it
    p1 = proj_primary.export_project_info().get('project_title')
    p2 = proj_secondary.export_project_info().get('project_title')
    logging.info(f'compare {project}:{p1} to {p2}')
    run_compare(proj_primary, proj_secondary, pdf_file, excel_file)

