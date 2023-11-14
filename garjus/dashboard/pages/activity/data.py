import logging
import os
from datetime import datetime

import pandas as pd

from .. import utils
from ....garjus import Garjus


logger = logging.getLogger('dashboard.activity.data')


# This is where we save our cache of the data
def get_filename():
    datadir = f'{Garjus.userdir()}/DATA'
    filename = f'{datadir}/activitydata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def get_data(proj_filter):
    df = pd.DataFrame()
    dfc = pd.DataFrame()
    dfq = pd.DataFrame()
    dfj = pd.DataFrame()

    # This week: monday of this week
    # import datetime
    # today = datetime.date.today()
    # startdate = today - datetime.timedelta(days=today.weekday())

    # This month: first date of current month
    # startdate = datetime.datetime.today().replace(day=1)

    # Past month
    from dateutil.relativedelta import relativedelta
    startdate = datetime.today() - relativedelta(months=1)
    startdate = startdate.strftime('%Y-%m-%d')

    g = Garjus()

    if not g.redcap_enabled():
        return None

    logger.info(f'loading activity:startdate={startdate}')
    dfc = g.activity(startdate=startdate)
    dfx = g.assessors()

    dfq = load_recent_qa(dfx, startdate=startdate)
    logger.info('loaded {} qa records'.format(len(dfq)))

    dfj = load_recent_jobs(dfx, startdate=startdate)
    logger.info('loaded {} job records'.format(len(dfj)))

    # Concatentate all the dataframes into one
    df = pd.concat([dfc, dfq, dfj], ignore_index=True)
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)
    df['ID'] = df.index

    return df


def load_recent_qa(df, startdate):
    enddate = datetime.today()
    enddate = enddate.strftime('%Y-%m-%d')

    df = df.copy()

    df['LABEL'] = df['ASSR']
    df['CATEGORY'] = df['PROCTYPE']

    # Filter by qc date
    df = df[(df.QCDATE >= startdate) & (df.QCDATE <= enddate)]

    df['STATUS'] = df['QCSTATUS'].map({
        'Failed': 'FAIL',
        'Passed': 'PASS'}).fillna('UNKNOWN')

    df['SOURCE'] = 'qa'

    df['CATEGORY'] = df['PROCTYPE']

    df['DESCRIPTION'] = 'QA' + ':' + df['LABEL']

    df['DATETIME'] = df['QCDATE']

    return df


def load_recent_jobs(df, startdate):
    df = df.copy()
    df['LABEL'] = df['ASSR']
    df['CATEGORY'] = df['PROCTYPE']

    # Filter by jobstartdate date, include anything with job running
    df = df[(df['JOBDATE'] >= startdate) | (df['PROCSTATUS'] == 'JOB_RUNNING')]

    df['STATUS'] = df['PROCSTATUS'].map({
        'COMPLETE': 'COMPLETE',
        'JOB_FAILED': 'FAIL',
        'JOB_RUNNING': 'NPUT'}).fillna('UNKNOWN')

    df['SOURCE'] = 'dax'

    df['CATEGORY'] = df['PROCTYPE']

    df['DESCRIPTION'] = 'JOB' + ':' + df['LABEL']

    df['DATETIME'] = df['JOBDATE']

    return df


def run_refresh(filename):
    proj_filter = []

    df = get_data(proj_filter)

    utils.save_data(df, filename)

    return df


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename)

    logger.info('reading data from file:{}'.format(filename))
    return utils.read_data(filename)


def filter_data(df, projects, categories, sources):
    # Filter by project
    if projects:
        logger.debug('filtering by project:')
        logger.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by category
    if categories:
        logger.debug('filtering by category:')
        logger.debug(categories)
        df = df[(df['CATEGORY'].isin(categories))]

    # Filter by source
    if sources:
        logger.debug('filtering by source:')
        logger.debug(sources)
        df = df[(df['SOURCE'].isin(sources))]

    return df
