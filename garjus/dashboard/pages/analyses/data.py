import logging
import os
import pandas as pd

from ....garjus import Garjus


logger = logging.getLogger('dashboard.analyses.data')


def get_filename():
    datadir = f'{Garjus.userdir()}/DATA'
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
    g = Garjus()

    if not g.redcap_enabled():
        logger.debug('redcap not enabled, no data')
        return pd.DataFrame(columns=g.column_names('analyses'))

    # Get the pid of the main redcap so we can make links
    pid = g.redcap_pid()

    # Load
    df = g.analyses(projects, download=False)

    # Make edit link
    df['EDIT'] = 'https://redcap.vanderbilt.edu/redcap_v13.9.3/DataEntry/index.php?pid=' + \
        str(pid) + \
        '&page=analyses&id=' + \
        df['PROJECT'] + \
        '&instance=' + \
        df['ID'].astype(str)

    return df


def filter_data(df, time=None):
    # Filter
    if time:
        pass

    return df
