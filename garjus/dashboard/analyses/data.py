import logging
import os
from datetime import datetime
import pandas as pd

from ...garjus import Garjus


logger = logging.getLogger('dashboard.analyses.data')


def get_filename():
    datadir = 'DATA'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)

    filename = f'{datadir}/analysesdata.pkl'
    return filename


def run_refresh(filename, projects):

    # force a requery
    df = get_data(projects)

    save_data(df, filename)

    return df


def load_options():
    garjus = Garjus()
    proj_options = garjus.projects()

    return proj_options


def load_data(projects, refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        # TODO: check for old file and refresh too
        run_refresh(filename, projects)

    logger.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def read_data(filename):
    df = pd.read_pickle(filename)
    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def get_data(projects):
    df = pd.DataFrame()
    garjus = Garjus()

    # Load
    df = garjus.analyses(projects)

    return df


def filter_data(df, time=None):
    # Filter by project
    if time:
        #logger.debug('filtering by project:')
        #logger.debug(projects)
        #df = df[df['PROJECT'].isin(projects)]
        print('TBD:time filter')


    return df
