import logging
import os
from datetime import datetime
import pandas as pd

from ...garjus import Garjus


logger = logging.getLogger('dashboard.analyses.data')


def get_filename():
    datadir = f'{Garjus().cachedir()}/DATA'
    filename = f'{datadir}/analysesdata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def run_refresh(filename, projects):
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
    # Filter
    if time:
        pass

    return df
