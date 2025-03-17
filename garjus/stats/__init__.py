"""

update will create any missing

"""
import tempfile
import csv
import logging
import glob
import os

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


def update(garjus, projects, proctypes=None):
    """Update project."""
    if not garjus.xnat_enabled():
        logger.debug('no xnat, cannot update stats')
        return

    for p in projects:
        if proctypes:
            ptypes = proctypes
        else:
            ptypes = garjus.stattypes(p)

        if not ptypes:
            logger.debug(f'no proctypes for stats project:{p}')
            continue

        logger.debug(f'stats updating project:{p},proctypes={ptypes}')
        update_project(garjus, p, ptypes)

        if 'BrainAgeGap_v2' in ptypes:
            logger.debug('getting bag_age_gap')
            _get_bag(garjus, p)


def update_project(garjus, project, proctypes):
    """Update stats for project proctypes."""

    logger.debug(f'loading existing stats:{project}')
    try:
        # Get list of assessors already in stats
        existing = garjus.stats_assessors(project, proctypes)
    except Exception:
        logger.debug(f'no stats loaded, check key:{project}')
        return

    # Get list of all assessors
    logger.debug(f'loading existing assessors:{project}')

    dfa = garjus.assessors([project], proctypes)
    logger.debug(f'total assessors:{len(dfa)}')

    # Remove any without stats resource
    logger.debug(f'loading assessor resource list:{project}')
    assessor_resources = garjus.assessor_resources(project, '')
    dfa['RESOURCES'] = dfa.apply(lambda x: assessor_resources.get(x.ASSR, ''), axis=1)
    dfa = dfa[dfa.RESOURCES.str.contains('STATS')]
    logger.debug(f'assessors after filtering out no STATS:{len(dfa)}')

    # Filter to remove already uploaded
    dfa = dfa[~dfa['ASSR'].isin(existing)]
    logger.debug(f'assessors after filtering out already uploaded:{len(dfa)}')

    # Filter to only COMPLETE
    dfa = dfa[dfa['PROCSTATUS'] == 'COMPLETE']
    logger.debug(f'assessors after filtering only COMPLETE:{len(dfa)}')

    # Filter to not Failed
    dfa = dfa[dfa['QCSTATUS'] != 'Failed']
    logger.debug(f'assessors after filtering out QC Failed:{len(dfa)}')

    # Iterate xnat assessors
    for r in dfa.sort_values('ASSR').to_dict('records'):
        try:
            update_assessor(
                garjus,
                r['PROJECT'],
                r['SUBJECT'],
                r['SESSION'],
                r['ASSR'],
            )
        except ConnectionError as err:
            logger.info(err)
            logger.info('waiting a minute')
            os.sleep(60)

    # Subject Assessors
    dfa = garjus.subject_assessors([project], proctypes)
    logger.debug(f'total sgp assessors:{len(dfa)}')

    # Remove any without stats resource
    logger.debug(f'loading sgp resource list:{project}')
    assessor_resources = garjus.subject_assessor_resources(project)
    dfa['RESOURCES'] = dfa.apply(lambda x: assessor_resources.get(x.ASSR, ''), axis=1)
    dfa = dfa[dfa.RESOURCES.str.contains('STATS')]
    logger.debug(f'assessors after filtering out no STATS:{len(dfa)}')

    # Filter to remove already uploaded
    dfa = dfa[~dfa['ASSR'].isin(existing)]
    logger.debug(f'subject assessors after filtering out already uploaded:{len(dfa)}')

    # Filter to only COMPLETE
    dfa = dfa[dfa['PROCSTATUS'] == 'COMPLETE']
    logger.debug(f'subject assessors after filtering only COMPLETE:{len(dfa)}')

    # Filter to not Failed
    dfa = dfa[dfa['QCSTATUS'] != 'Failed']
    logger.debug(f'subject assessors after filtering out QC Failed:{len(dfa)}')

    # Iterate xnat assessors
    for r in dfa.sort_values('ASSR').to_dict('records'):
        try:
            update_subject_assessor(
                garjus,
                r['PROJECT'],
                r['SUBJECT'],
                r['ASSR'],
            )
        except ConnectionError as err:
            logger.info(err)
            logger.info('waiting a minute')
            os.sleep(60)


def update_assessor(garjus, proj, subj, sess, assr):
    """Update assessor stats."""
    logger.debug(f'uploading assessor stats:{assr}')
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            _dir = garjus.get_source_stats(proj, subj, sess, assr, tmpdir)
        except Exception as err:
            logger.warn(f'could not get stats:{assr}:{err}')
            return

        _stats = transform_stats(_dir)
        garjus.set_stats(proj, subj, sess, assr, _stats)


def update_subject_assessor(garjus, proj, subj, assr):
    """Update subject assessor stats."""
    logger.debug(f'uploading subject assessor stats:{assr}')

    with tempfile.TemporaryDirectory() as tmpdir:
        try:    
            logger.debug(f'{proj}:{subj}:{assr}:{tmpdir}')
            _dir = garjus.get_sgp_source_stats(proj, subj, assr, tmpdir)
        except Exception as err:
            logger.warn(f'could not get stats:{assr}:{err}')
            import traceback
            traceback.print_exc()
            return

        _stats = transform_stats(_dir)
        garjus.set_sgp_stats(proj, subj, assr, _stats)


def transform_stats(stats_dir):
    """Transform stats from directory of files to dict."""
    data = {}

    if os.path.exists(f'{stats_dir}/stats.csv'):
        data = _load_stats(f'{stats_dir}/stats.csv')
    elif os.path.exists(f'{stats_dir}/stats.txt'):
        data = _load_stats(f'{stats_dir}/stats.txt')
    elif os.path.exists(f'{stats_dir}/fmriqa_stats.csv'):
        data.update(_load_stats_tall(f'{stats_dir}/fmriqa_stats.csv'))
    elif len(glob.glob(f'{stats_dir}/*.txt')) > 0:
        for txt_path in glob.iglob(f'{stats_dir}/*.txt'):
            data.update(_load_stats_tall(txt_path))
    else:
        # Handle proctypes that output multiple csv
        for csv_path in glob.iglob(f'{stats_dir}/*.csv'):
            data.update(_load_stats_wide(csv_path))

    return data


def _isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def _load_stats_wide(filename):
    data = {}

    with open(filename, newline='') as f:
        # Connect csv reader
        reader = csv.reader(f)

        # Load header from first line
        header = next(reader)

        # Read data from subsequent lines
        for line in reader:
            for i, v in enumerate(line):
                data[header[i]] = v

    return data


def _load_stats_tall(filename):
    data = {}
    rows = []

    try:
        with open(filename) as f:
            rows = f.readlines()

        for r in rows:
            (k, v) = r.strip().replace('=', ',').split(',')
            data[k] = v
    except ValueError:
        logger.error(f'cannot load stats file:{filename}')
        return {}

    return data


def _load_stats(filename):
    lines = []
    with open(filename) as f:
        lines = f.readlines()

    if ('=' in lines[0]) or (len(lines) >= 3 and len(lines[0].split(',')) <= 3):
        return _load_stats_tall(filename)
    else:
        return _load_stats_wide(filename)


def _get_bag(garjus, project):

    # Get subjects with DOB
    subjects = garjus.subjects(project, include_dob=True)
    if 'DOB' not in subjects.columns:
        logger.debug('DOB not found, cannot calculate bag_age_gap')
        return

    # Get BAG stats
    stats = garjus.stats(project, proctypes=['BrainAgeGap_v2'])

    if stats.empty:
        return

    # Merge in DOB
    subjects['SUBJECT'] = subjects['ID']
    stats = pd.merge(stats, subjects[['SUBJECT','DOB']], on='SUBJECT')

    if 'bag_age_gap' in stats:
        # Only rows without existing bag_age_gap
        stats = stats[stats.bag_age_gap.isna()]

    # Only rows with DOB
    stats = stats[~stats.DOB.isna()]

    # Calculate age at scan
    stats['SCANDAYS'] = pd.to_datetime(stats['DATE']) - stats['DOB']
    stats['BAGDAYS'] = (stats['bag_age_pred'].astype(float) * 365.25).astype('timedelta64[D]')
    stats['bag_age_gap'] = (stats['BAGDAYS'] - stats['SCANDAYS'])/np.timedelta64(365, 'D')

    # Batch upload new stats
    for i, s in stats.iterrows():
        logger.debug(f'set bag_age_gap:{s.ASSR}')
        garjus.set_stats(
            project,
            s.SUBJECT,
            s.SESSION,
            s.ASSR,
            {'bag_age_gap': s.bag_age_gap})


def _get_bag_nodob(garjus, project):
    subjects = garjus.subjects(project)

    # Get BAG stats
    stats = garjus.stats(project, proctypes=['BrainAgeGap_v2'])

    if stats.empty:
        return

    stats = pd.merge(
        stats, subjects[['AGE']], left_on='SUBJECT', right_index=True)

    if 'bag_age_gap' in stats:
        # Only rows without existing bag_age_gap
        stats = stats[stats.bag_age_gap.isna()]

    stats['bag_age_gap'] = (stats['bag_age_pred'].astype(float) - stats['AGE'].astype(float))

    # Batch upload new stats
    for i, s in stats.iterrows():
        logger.debug(f'set bag_age_gap:{s.ASSR}')
        garjus.set_stats(
            project,
            s.SUBJECT,
            s.SESSION,
            s.ASSR,
            {'bag_age_gap': s.bag_age_gap})

