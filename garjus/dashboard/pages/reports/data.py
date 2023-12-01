import logging
import os
import pandas as pd
from datetime import datetime

from ....garjus import Garjus


logger = logging.getLogger('dashboard.reports.data')


def get_filename():
    datadir = f'{Garjus.userdir()}/DATA'
    filename = f'{datadir}/reportsdata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def run_refresh(filename, projects):
    df = get_data(projects)

    save_data(df, filename)

    return df


def load_options(df):
    projects = []
    types = []
    times = ['All', 'Current']

    # Projects
    garjus = Garjus()
    projects = garjus.projects()

    # Selected types
    types = df.TYPE.unique()

    # Remove blanks and sort
    types = [x for x in types if x]
    types = sorted(types)

    return projects, types, times


def load_data(projects, types, timeframe, refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename, projects)

    logger.info('reading data from file:{}'.format(filename))
    df = read_data(filename)

    if types:
        df = df[df.TYPE.isin(types)]

    if timeframe == 'Current':
        cur_double = datetime.now().strftime("%B%Y")
        df = df[df.NAME == cur_double]

    return df


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def get_data(projects):
    garjus = Garjus()

    # Get the pid of the main redcap so we can make links
    pid = garjus.redcap_pid()

    # Load
    df = garjus.reports(projects)

    # Make pdf link
    df['VIEW'] = 'https://redcap.vanderbilt.edu/redcap_v14.0.0/DataEntry/index.php?' + \
    'pid=' + str(pid) + \
    '&page=' + df.TYPE.str.lower() + \
    '&id=' + df['PROJECT'] + \
    '&instance=' + df['ID'].astype(str)

    return df


def filter_data(df, time=None):
    # Filter
    if time:
        pass

    return df
