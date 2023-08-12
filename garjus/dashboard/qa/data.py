import logging
import os
from datetime import datetime, date, timedelta

import pandas as pd

from ...garjus import Garjus


logger = logging.getLogger('dashboard.qa.data')


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
    'ARTTYPE', 'SCANTYPE', 'PROCTYPE', 'XSITYPE', 'SESSTYPE', 'MODALITY']


def get_filename():
    datadir = 'DATA'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)

    filename = f'{datadir}/qadata.pkl'
    return filename


def run_refresh(filename, hidetypes=True):
    proj_filter = []
    proc_filter = []
    scan_filter = []

    # force a requery
    df = get_data(proj_filter, proc_filter, scan_filter, hidetypes=hidetypes)

    save_data(df, filename)

    return df


# TODO: combine these load_x_options to only read the file once
def load_scan_options(project_filter=None):
    # Read stypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logger.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logger.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        scantypes = df[df.PROJECT.isin(project_filter)].SCANTYPE.unique()
    else:
        scantypes = df.SCANTYPE.unique()

    scantypes = [x for x in scantypes if x]

    return sorted(scantypes)


# TODO: combine these load_x_options to only read the file once
def load_sess_options(project_filter=None):
    # Read stypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logger.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logger.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        sesstypes = df[df.PROJECT.isin(project_filter)].SESSTYPE.unique()
    else:
        sesstypes = df.SESSTYPE.unique()

    sesstypes = [x for x in sesstypes if x]

    return sorted(sesstypes)


def load_proc_options(project_filter=None):
    # Read ptypes from file and filter by projects

    filename = get_filename()

    if not os.path.exists(filename):
        logger.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logger.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    if project_filter:
        proctypes = df[df.PROJECT.isin(project_filter)].PROCTYPE.unique()
    else:
        proctypes = df.PROCTYPE.unique()

    proctypes = [x for x in proctypes if x]

    return sorted(proctypes)


def load_proj_options():
    filename = get_filename()

    if not os.path.exists(filename):
        logger.debug('refreshing data for file:{}'.format(filename))
        run_refresh()

    logger.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    return sorted(df.PROJECT.unique())


def load_data(refresh=False, hidetypes=True):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        # TODO: check for old file and refresh too
        run_refresh(filename, hidetypes)

    logger.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def get_data(proj_filter, stype_filter, ptype_filter, hidetypes=True):
    try:
        garjus = Garjus()

        # Load that data
        scan_df = load_scan_data(garjus, proj_filter)
        assr_df = load_assr_data(garjus, proj_filter)

    except Exception as err:
        logger.error(err)
        return pd.DataFrame(columns=QA_COLS+['DATE', 'SESSIONLINK'])

    if hidetypes:
        logger.info('applying filter types')
        scan_df, assr_df = filter_types(garjus, scan_df, assr_df)

    # Make a common column for type
    assr_df['TYPE'] = assr_df['PROCTYPE']
    scan_df['TYPE'] = scan_df['SCANTYPE']

    assr_df['SCANTYPE'] = None
    scan_df['PROCTYPE'] = None

    assr_df['ARTTYPE'] = 'assessor'
    scan_df['ARTTYPE'] = 'scan'

    # Concatenate the common cols to a new dataframe
    df = pd.concat([assr_df[QA_COLS], scan_df[QA_COLS]], sort=False)

    # relabel caare, etc
    df.PROJECT = df.PROJECT.replace(['TAYLOR_CAARE'], 'CAARE')
    df.PROJECT = df.PROJECT.replace(['TAYLOR_DepMIND'], 'DepMIND1')

    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')

    df['SESSIONLINK'] = garjus.xnat().host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    return df


def filter_types(garjus, scan_df, assr_df):
    scantypes = []
    assrtypes = []

    # Load types
    logger.info('loading scan/assr types')
    scantypes = garjus.all_scantypes()
    assrtypes = garjus.all_proctypes()

    # Make the lists unique
    scantypes = list(set(scantypes))
    assrtypes = list(set(assrtypes))

    # Apply filters
    logger.info(f'filtering by types:{len(scan_df)}:{len(assr_df)}')
    scan_df = scan_df[scan_df['SCANTYPE'].isin(scantypes)]
    assr_df = assr_df[assr_df['PROCTYPE'].isin(assrtypes)]
    logger.info(f'done filtering by types:{len(scan_df)}:{len(assr_df)}')

    return scan_df, assr_df


def load_assr_data(garjus, project_filter):
    dfa = garjus.assessors().copy()

    # Get subset of columns
    dfa = dfa[[
        'PROJECT', 'SESSION', 'SUBJECT', 'NOTE', 'DATE', 'SITE', 'ASSR', 'QCSTATUS',
        'PROCSTATUS', 'PROCTYPE', 'XSITYPE', 'SESSTYPE', 'MODALITY']]

    dfa.drop_duplicates(inplace=True)

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


def load_scan_data(garjus, project_filter):
    #  Load data
    dfs = garjus.scans()

    dfs = dfs[[
        'PROJECT', 'SESSION', 'SUBJECT', 'NOTE', 'DATE', 'SITE', 'SCANID',
        'SCANTYPE', 'QUALITY', 'XSITYPE', 'SESSTYPE', 'MODALITY']].copy()
    dfs.drop_duplicates(inplace=True)

    # Drop any rows with empty type
    dfs.dropna(subset=['SCANTYPE'], inplace=True)
    dfs = dfs[dfs.SCANTYPE != '']

    # Create shorthand status
    dfs['STATUS'] = dfs['QUALITY'].map(SCAN_STATUS_MAP).fillna('U')

    return dfs


def filter_data(df, projects, proctypes, scantypes, timeframe, sesstypes):

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
        df = df[(df['SCANTYPE'].isin(scantypes)) | (df['ARTTYPE'] == 'assessor')]

    # Filter by timeframe
    if timeframe in ['1day', '7day', '30day', '365day']:
        logger.debug('filtering by ' + timeframe)
        then_datetime = datetime.now() - pd.to_timedelta(timeframe)
        df = df[pd.to_datetime(df.DATE) > then_datetime]
    elif timeframe == 'lastmonth':
        logger.debug('filtering by ' + timeframe)

        # Set range to first and last day of previous month
        _end = date.today().replace(day=1) - timedelta(days=1)
        _start = date.today().replace(day=1) - timedelta(days=_end.day)
        df = df[pd.to_datetime(df.DATE).isin(pd.date_range(_start, _end))]
    else:
        # ALL
        logger.debug('not filtering by time')
        pass

    # Filter by sesstype
    if sesstypes:
        df = df[df['SESSTYPE'].isin(sesstypes)]

    return df
