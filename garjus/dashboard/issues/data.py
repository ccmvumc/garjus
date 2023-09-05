import logging
import os

import pandas as pd

from ...garjus import Garjus


logger = logging.getLogger('dashboard.issues.data')


# This is where we save our cache of the data
def get_filename():
    datadir = f'{Garjus().cachedir()}/DATA'
    filename = f'{datadir}/issuesdata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def get_data():
    logger.info('loading issues')
    df = load_garjus_issues()

    # Sort by date and reset index
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)
    df['ID'] = df.index
    df['STATUS'] = 'FAIL'
    df['LABEL'] = df['ID']

    return df


def load_garjus_issues():
    g = Garjus()

    return g.issues()


def run_refresh(filename):
    df = get_data()

    if not df.empty:
        save_data(df, filename)


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename)

    logger.info('reading data from file:{}'.format(filename))
    return read_data(filename)


def read_data(filename):

    if os.path.exists(filename):
        df = pd.read_pickle(filename)
    else:
         df = pd.DataFrame(columns=[
            'ID', 'LABEL', 'PROJECT', 'SUBJECT', 'SESSION',
            'EVENT', 'FIELD', 'CATEGORY', 'STATUS',
            'DESCRIPTION', 'DATETIME'
        ])

    return df


def save_data(df, filename):
    # save to cache
    df.to_pickle(filename)


def filter_data(df, projects, categories):
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

    return df
