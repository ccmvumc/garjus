"""QA Dashboard."""
import logging
import os
import time

import pandas as pd

from ...garjus import Garjus


logger = logging.getLogger('dashboard.qa.data')


# TODO: modify save and filter so we save the data before filtering,
# then we don't have to refresh or really do anything, either filter is on or
# off. problem is we are filtering before merging scans/assessors so
# need to refactor that. for now it will be 2 clicks to change to autofilter
# including refresh click.

SCAN_STATUS_MAP = {
    'usable': 'P',
    'questionable': 'P',
    'unusable': 'F'}


ASSR_STATUS_MAP = {
    'Passed': 'P',
    'Good': 'P',
    'Passed with edits': 'P',
    'Questionable': 'P',
    'Failed': 'F',
    'Bad': 'F',
    'Needs QA': 'Q',
    'Do Not Run': 'N'}


QA_COLS = [
    'SESSION', 'SUBJECT', 'PROJECT',
    'SITE', 'NOTE', 'DATE', 'TYPE', 'STATUS',
    'ARTTYPE', 'SCANTYPE', 'PROCTYPE', 'XSITYPE', 'SESSTYPE',
    'MODALITY']


def get_filename():
    datadir = f'{Garjus().cachedir()}/DATA'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    filename = f'{datadir}/qadata.pkl'

    return filename


def run_refresh(projects, hidetypes=True):
    filename = get_filename()

    # force a requery
    df = get_data(projects, hidetypes=hidetypes)

    save_data(df, filename)

    return df


def file_age(filename):
    return int((time.time() - os.path.getmtime(filename)) / 60)


def update_data(projects, hidetypes):
    fname = get_filename()

    # Load what we have now
    df = read_data(fname)

    # Remove projects not selected
    df = df[df.PROJECT.isin(projects)]

    # Find new projects in selected
    new_projects = [x for x in projects if x not in df.PROJECT.unique()]

    if new_projects:

        # Save a file with new projects placeholders (hacky lock)
        for p in new_projects:
            _newdf = pd.DataFrame.from_records([{'PROJECT': p}])
            df = pd.concat([df, _newdf], ignore_index=True)

        save_data(df, fname)

        # Load the new projects
        dfp = get_data(new_projects, hidetypes=hidetypes)

        # Remove our "lock" rows
        df = df[~df.PROJECT.isin(new_projects)]

        # Merge our new data with old data
        df = pd.concat([df, dfp])

        # Save it to file
        save_data(df, fname)

    return df


def load_data(projects=[], refresh=False, maxmins=60, hidetypes=True):
    fname = get_filename()

    if not os.path.exists(fname):
        refresh = True
    elif file_age(fname) > maxmins:
        logger.info(f'refreshing, file age limit reached:{maxmins} minutes')
        refresh = True

    if refresh:
        df = run_refresh(projects, hidetypes)
    elif set(projects) != set(read_data(fname).PROJECT.unique()):
        logger.debug('updating data')
        # Different projects selected, update
        df = update_data(projects, hidetypes)
    else:
        df = read_data(fname)
        df = df[df['PROJECT'].isin(projects)]

    df = df.dropna(subset=['TYPE'])

    return df


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def get_data(projects, hidetypes=True):
    df = pd.DataFrame(columns=QA_COLS)

    if not projects:
        # No projects selected so we don't query
        return df

    try:
        garjus = Garjus()

        # Load data
        logger.info(f'load data:{projects}')
        logger.debug(f'load scan data:{projects}')
        scan_df = load_scan_data(garjus, projects)
        logger.debug(f'load assr data:{projects}')
        assr_df = load_assr_data(garjus, projects)
        logger.debug(f'load sgp data:{projects}')
        subj_df = load_sgp_data(garjus, projects)

    except Exception as err:
        logger.error(err)
        _cols = QA_COLS + ['DATE', 'SESSIONLINK', 'SUBJECTLINK']
        return pd.DataFrame(columns=_cols)

    logger.debug(f'merging data:{projects}')
    if hidetypes:
        logger.debug('applying autofilter to hide unused types')
        scantypes = None
        assrtypes = None

        if garjus.redcap_enabled():
            # Load types
            logger.debug('loading scan/assr types')
            scantypes = garjus.all_scantypes()
            assrtypes = garjus.all_proctypes()

            # Make the lists unique
            scantypes = list(set(scantypes))
            assrtypes = list(set(assrtypes))

        if not scantypes and not assr_df.empty:
            # Get list of scan types based on assessor inputs
            logger.debug('loading used scan types')
            scantypes = garjus.used_scantypes(assr_df, scan_df)

        scan_df, assr_df = _filter(scan_df, assr_df, scantypes, assrtypes)

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']

    assr_df['SCANTYPE'] = None
    scan_df['PROCTYPE'] = None

    assr_df['ARTTYPE'] = 'assessor'
    scan_df['ARTTYPE'] = 'scan'

    # Concatenate the common cols to a new dataframe
    df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)

    subj_df['TYPE'] = subj_df['PROCTYPE']
    subj_df['SCANTYPE'] = None
    subj_df['ARTTYPE'] = 'sgp'
    subj_df['SESSION'] = subj_df['ASSR']
    subj_df['SITE'] = 'SGP'
    subj_df['NOTE'] = ''
    subj_df['SESSTYPE'] = 'SGP'
    subj_df['MODALITY'] = 'SGP'
    df = pd.concat([df[QA_COLS], subj_df[QA_COLS]], sort=False)

    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')

    df['SESSIONLINK'] = garjus.xnat().host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    df['SUBJECTLINK'] = garjus.xnat().host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT']

    return df


def _filter(scan_df, assr_df, scantypes, assrtypes):

    # Apply filters
    if scantypes is not None:
        logger.debug(f'filtering scan by types:{len(scan_df)}')
        scan_df = scan_df[scan_df['SCANTYPE'].isin(scantypes)]

    if assrtypes is not None:
        logger.debug(f'filtering assr by types:{len(assr_df)}')
        assr_df = assr_df[assr_df['PROCTYPE'].isin(assrtypes)]

    logger.debug(f'done filtering by types:{len(scan_df)}:{len(assr_df)}')

    return scan_df, assr_df


def load_assr_data(garjus, project_filter):
    dfa = garjus.assessors(project_filter).copy()

    # Get subset of columns
    #print('subset?')
    #dfa = dfa[[
    #    'PROJECT', 'SESSION', 'SUBJECT', 'ASSR',
    #    'NOTE', 'DATE', 'SITE', 'QCSTATUS', 'PROCSTATUS',
    #    'PROCTYPE', 'XSITYPE', 'SESSTYPE', 'MODALITY', 'INPUTS']]

    #dfa.drop_duplicates(inplace=True)

    # Drop any rows with empty proctype
    dfa.dropna(subset=['PROCTYPE'], inplace=True)
    dfa = dfa[dfa.PROCTYPE != '']

    # Create shorthand status
    dfa['STATUS'] = dfa['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # Handle failed jobs
    dfa.loc[dfa.PROCSTATUS == 'JOB_FAILED', 'STATUS'] = 'X'

    # Handle running jobs
    dfa.loc[dfa.PROCSTATUS == 'JOB_RUNNING', 'STATUS'] = 'R'

    # Handle NEED INPUTS
    dfa.loc[dfa.PROCSTATUS == 'NEED_INPUTS', 'STATUS'] = 'N'

    return dfa


def load_sgp_data(garjus, project_filter):

    df = garjus.subject_assessors(project_filter).copy()

    # Get subset of columns
    df = df[[
        'PROJECT', 'SUBJECT', 'DATE', 'ASSR', 'QCSTATUS', 'XSITYPE',
        'PROCSTATUS', 'PROCTYPE']]

    df.drop_duplicates(inplace=True)

    # Drop any rows with empty proctype
    df.dropna(subset=['PROCTYPE'], inplace=True)
    df = df[df.PROCTYPE != '']

    # Create shorthand status
    df['STATUS'] = df['QCSTATUS'].map(ASSR_STATUS_MAP).fillna('Q')

    # Handle failed jobs
    df.loc[df.PROCSTATUS == 'JOB_FAILED', 'STATUS'] = 'X'

    # Handle running jobs
    df.loc[df.PROCSTATUS == 'JOB_RUNNING', 'STATUS'] = 'R'

    # Handle NEED INPUTS
    df.loc[df.PROCSTATUS == 'NEED_INPUTS', 'STATUS'] = 'N'

    return df


def load_scan_data(garjus, project_filter):

    #  Load data
    dfs = garjus.scans(project_filter)

    dfs = dfs[[
        'PROJECT', 'SESSION', 'SUBJECT', 'NOTE', 'DATE', 'SITE', 'SCANID',
        'SCANTYPE', 'QUALITY', 'XSITYPE', 'SESSTYPE', 'MODALITY',
        'full_path']].copy()
    dfs.drop_duplicates(inplace=True)

    # Drop any rows with empty type
    dfs.dropna(subset=['SCANTYPE'], inplace=True)
    dfs = dfs[dfs.SCANTYPE != '']

    # Create shorthand status
    dfs['STATUS'] = dfs['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')

    return dfs


def filter_data(df, projects, proctypes, scantypes, starttime, endtime, sesstypes):

    # Filter by project
    if projects:
        logger.debug('filtering by project:')
        logger.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by proc type
    if proctypes:
        logger.debug('filtering by proc types:')
        logger.debug(proctypes)
        df = df[(df['PROCTYPE'].isin(proctypes)) | (df['ARTTYPE'] == 'scan')]

    # Filter by scan type
    if scantypes:
        logger.debug('filtering by scan types:')
        logger.debug(scantypes)
        df = df[(df['SCANTYPE'].isin(scantypes)) | (df['ARTTYPE'] == 'assessor') | (df['ARTTYPE'] == 'sgp')]

    if starttime:
        logger.debug(f'filtering by start time:{starttime}')
        df = df[pd.to_datetime(df.DATE) >= starttime]

    if endtime:
        df = df[pd.to_datetime(df.DATE) <= endtime]

    # Filter by sesstype
    if sesstypes:
        df = df[df['SESSTYPE'].isin(sesstypes)]

    return df
