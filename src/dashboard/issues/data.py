import logging
import os

import pandas as pd

from ...garjus import Garjus


# This is where we save our cache of the data
def get_filename():
    datadir = 'DATA'
    if not os.path.isdir(datadir):
        os.mkdir(datadir)

    filename = f'{datadir}/issuesdata.pkl'
    return filename


def get_data():
    logging.info('loading issues')
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
    proj_filter = []

    df = get_data()

    if not df.empty:
        save_data(df, filename)


def load_field_options(fieldname):
    filename = get_filename()

    if not os.path.exists(filename):
        logging.debug('refreshing data for file:{}'.format(filename))
        run_refresh(filename)

    logging.debug('reading data from file:{}'.format(filename))
    df = pd.read_pickle(filename)

    _options = df[fieldname].unique()

    _options = [x for x in _options if x]

    return sorted(_options)


def load_category_options():
    return load_field_options('CATEGORY')


def load_project_options():
    return load_field_options('PROJECT')


def load_data(refresh=False):
    filename = get_filename()

    if refresh or not os.path.exists(filename):
        run_refresh(filename)

    logging.info('reading data from file:{}'.format(filename))
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
        logging.debug('filtering by project:')
        logging.debug(projects)
        df = df[df['PROJECT'].isin(projects)]

    # Filter by category
    if categories:
        logging.debug('filtering by category:')
        logging.debug(categories)
        df = df[(df['CATEGORY'].isin(categories))]

    return df
