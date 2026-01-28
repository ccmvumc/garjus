"""
Create stats export with anonymized ids and dates
"""
from datetime import datetime
import tempfile
import logging
import os, shutil

import pandas as pd

from .export import make_export_report


logger = logging.getLogger('garjus.progress')


SUBJECTS_COLUMNS = ['ID', 'PROJECT', 'GROUP', 'AGE', 'SEX']


def apply_anon(subjects, stats, links):

    # Replace subject ID
    subjects = pd.merge(
        subjects,
        links[['ID', 'anon_id']].drop_duplicates(),
        how='left',
        left_on='ID',
        right_on='ID'
    )
    subjects['ID'] = subjects['anon_id'].astype(str)
    subjects = subjects.drop(columns=['anon_id'])

    # Replace ID and dates in stats
    stats['SUBJECT'] = stats['SUBJECT'].astype(str)
    stats['DATE'] = pd.to_datetime(stats['DATE'])
    links['ID'] = links['ID'].astype(str)
    links['mri_date'] = pd.to_datetime(links['mri_date'])
    links['anon_date'] = pd.to_datetime(links['anon_date'])
    stats = pd.merge(
        stats,
        links,
        how='left',
        left_on=['SUBJECT', 'DATE'],
        right_on=['ID', 'mri_date'],
    )
    stats['SUBJECT'] = stats['anon_id'].astype(str)
    stats['DATE'] = stats['anon_date']
    stats = stats.sort_values(['SUBJECT', 'DATE', 'PROCTYPE'])
    stats = stats.drop(columns=['ID', 'anon_id', 'mri_date', 'anon_date', 'redcap_event_name'])
    stats = stats.drop(columns=['ASSR', 'SESSION'])
    stats['ASSR'] = stats.index.astype(str)
    return subjects, stats


def _get_links(garjus, project):
    # Get table linking IDs and dates
    return garjus.load_linked(project)


def _get_data(garjus, project):
    # Load subjects
    subjects = garjus.subjects(project).reset_index()

    # Only include specific subset of columns
    subjects = subjects[[x for x in subjects.columns if x in SUBJECTS_COLUMNS]]

    # Load stats
    stats = garjus.stats(project)
    if len(stats) > 0:
        # Only stats for subjects with row in subjects table
        stats = stats[stats.SUBJECT.isin(subjects.ID.unique())]

        # Only subjects with rows in stats table
        subjects = subjects[subjects.ID.isin(stats.SUBJECT.unique())]

        # Anonymize by replacing ids and dates
        logger.info('applying anonymization')

        # Load anonymized subject table
        links = _get_links(garjus, project)

        # Apply link table to replace ids/dates
        subjects, stats = apply_anon(subjects, stats, links)

    return subjects, stats


def make_anonshot(
    garjus,
    project,
    anonproject
):
    """Export stats and upload results as a new analysis."""
    subjects, stats = _get_data(garjus, project)

    # Check for empty
    if len(stats) == 0:
        logger.warning(f'no stats for project:{project}')
        return

    if len(subjects) == 0:
        logger.info('no subject data found, using list from stats')
        subjects = pd.DataFrame({'ID': stats.SUBJECT.unique()})
        subjects['PROJECT'] = project
        subjects['GROUP'] = 'UNKNOWN'

    # Make PITT be UPMC
    stats['SITE'] = stats['SITE'].replace({'PITT': 'UPMC'})
    if 'SITE' in subjects.columns:
        subjects['SITE'] = subjects['SITE'].replace({'PITT': 'UPMC'})

    with tempfile.TemporaryDirectory() as tmpdir:
        # Save subjects csv
        csv_file = os.path.join(tmpdir, f'subjects.csv')
        logger.info(f'saving subjects csv:{csv_file}')
        subjects.to_csv(csv_file, index=False)

        # Save other csv data types stored in REDCap
        covar = garjus.export_covariates(tmpdir, subjects)

        pdf_file = os.path.join(tmpdir, 'report.pdf')
        make_export_report(pdf_file, garjus, subjects, stats, covar)

        # Save a csv for each proc type
        for proctype in stats.PROCTYPE.unique():
            # Get the data for this processing type
            dft = stats[stats.PROCTYPE == proctype]

            dft = dft.dropna(axis=1, how='all')

            dft = dft.drop(columns=['ASSR'])

            dft = dft.reset_index(drop=True)

            # Save file for this type
            csv_file = os.path.join(tmpdir, f'{proctype}.csv')
            logger.info(f'saving csv:{proctype}:{csv_file}')
            dft.to_csv(csv_file, index=False)

        # Creates new analysis on redcap with files uploaded to xnat
        logger.info(f'upload analysis:{project=}:{anonproject=}')
        upload_analysis(garjus, project, anonproject, tmpdir)


def upload_analysis(garjus, project, anonproject, analysis_dir):
    # Create new record analysis
    analysis_name = f'anonshot:{project}'
    analysis_id = garjus.add_analysis(anonproject, analysis_dir, analysis_name)
    return analysis_id
