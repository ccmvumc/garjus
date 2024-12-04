"""

Manage progress reports. Update will create any missing.

"""
from datetime import datetime
import tempfile
import logging
import os, shutil

import pandas as pd

from .report import make_project_report
from .export import make_export_report


logger = logging.getLogger('garjus.progress')


# class dataarchive
# -projects
# -proctypes
# -sesstypes


SUBJECTS_COLUMNS = ['ID', 'PROJECT', 'GROUP', 'AGE', 'SEX']


def update(garjus, projects=None):
    """Update project progress."""
    if not garjus.xnat_enabled():
        logger.debug('no xnat, cannot update progress')
        return

    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating progress:{p}')
            update_project(garjus, p)


def update_project(garjus, project):
    """Update project progress."""
    progs = garjus.progress_reports(projects=[project])

    # what time is it? we will use this for naming
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
        zip_file = f'{outdir}/{project}_data_{fnow}.zip'

        make_project_report(garjus, project, pdf_file, zip_file, monthly=True)
        garjus.add_progress(project, cur_progress, now, pdf_file, zip_file)


def make_export_zip(garjus, filename, projects, proctypes, sesstypes, sessions):
    stats = pd.DataFrame()
    subjects = pd.DataFrame()

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
        psubjects = garjus.subjects(p).reset_index()

        # Load project stats
        pstats = garjus.stats(p, proctypes=proctypes, sesstypes=sesstypes)

        # Check for empty
        if len(pstats) == 0:
            logger.info(f'no stats for project:{p}')
            continue

        # Append to total
        subjects = pd.concat([subjects, psubjects])
        stats = pd.concat([stats, pstats])

    # Filter duplicate GUID to handle same subject in multiple projects
    subjects = subjects[(subjects['GUID'] == '') | (subjects['GUID'].isna()) | ~subjects.duplicated(subset='GUID')]

    # Only include specifc subset of columns
    subjects = subjects[SUBJECTS_COLUMNS]

    # Pivot table to count occurrences of each type for each subject
    dfp = stats.pivot_table(index='SUBJECT', columns='PROCTYPE', aggfunc='size', fill_value=0)
    valid_subjects = dfp[(dfp > 0).all(axis=1)].index
    subjects = subjects[subjects.ID.isin(valid_subjects)]

    # Only stats for subjects in subjects
    stats = stats[stats.SUBJECT.isin(subjects.ID.unique())]

    # Make PITT be UPMC
    stats['SITE'] = stats['SITE'].replace({'PITT': 'UPMC'})
    if 'SITE' in subjects.columns:
        subjects['SITE'] = subjects['SITE'].replace({'PITT': 'UPMC'})

    with tempfile.TemporaryDirectory() as tmpdir:
        # Prep output dir
        data_dir = os.path.join(tmpdir, 'data')
        zip_file = os.path.join(tmpdir, 'data.zip')
        stats_dir = os.path.join(data_dir, 'stats')
        pdf_file = os.path.join(data_dir, 'report.pdf')
        os.mkdir(data_dir)
        os.mkdir(stats_dir)

        make_export_report(pdf_file, garjus, subjects, stats)

        # Save subjects csv
        csv_file = os.path.join(data_dir, f'subjects.csv')
        logger.info(f'saving subjects csv:{csv_file}')
        subjects.to_csv(csv_file, index=False)

        # Save a csv for each proc type
        for proctype in stats.PROCTYPE.unique():
            # Get the data for this processing type
            dft = stats[stats.PROCTYPE == proctype]

            dft = dft.dropna(axis=1, how='all')

            dft = dft.sort_values('ASSR')

            # Save file for this type
            csv_file = os.path.join(stats_dir, f'{proctype}.csv')
            logger.info(f'saving csv:{proctype}:{csv_file}')
            dft.to_csv(csv_file, index=False)

        # Create zip file of dir of csv files
        shutil.make_archive(data_dir, 'zip', data_dir)


        # Save it outside of temp dir
        logger.info(f'saving zip:{filename}')
        shutil.copy(zip_file, filename)


def make_stats_csv(
    garjus,
    projects,
    proctypes,
    sesstypes,
    csvname,
    persubject=False,
    analysis=None,
    sessions=None
):
    """"Make the file."""
    df = pd.DataFrame()

    if not isinstance(projects, list):
        projects = projects.split(',')

    if proctypes is not None and not isinstance(proctypes, list):
        proctypes = proctypes.split(',')

    if sesstypes is not None and not isinstance(sesstypes, list):
        sesstypes = sesstypes.split(',')

    if sessions is not None and not isinstance(sessions, list):
        sessions = sessions.split(',')

    for p in sorted(projects):
        # Load stats
        stats = garjus.stats(
            p, proctypes=proctypes, sesstypes=sesstypes, persubject=persubject)
        df = pd.concat([df, stats])

    if analysis:
        # Get the list of subjects for specified analysis and apply as filter
        logger.info(f'{analysis=}')

        # Get the subject list from the analysis
        project, analysis_id = analysis.rsplit('_', 1)
        a = garjus.load_analysis(project, analysis_id)
        subjects = a['SUBJECTS'].splitlines()
        logger.debug(f'applying subject filter to include:{subjects}')
        df = df[df.SUBJECT.isin(subjects)]

        # Append rows for missing subjects and resort
        _subj = df.SUBJECT.unique()
        missing_subjects = [x for x in subjects if x not in _subj]
        if missing_subjects:
            logger.info(f'{missing_subjects=}')
            df = pd.concat([
                df,
                pd.DataFrame(
                    missing_subjects,
                    columns=['SUBJECT']
                )
            ]).sort_values('SUBJECT')

    if sessions:
        df = df[df.SESSION.isin(sessions)]
        logger.info(f'filter sessions:{sessions}')

    # Save file for this type
    logger.info(f'saving csv:{csvname}')
    df.to_csv(csvname, index=False)
