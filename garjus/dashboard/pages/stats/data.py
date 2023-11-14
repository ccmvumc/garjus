import logging
import os
from datetime import datetime
import pandas as pd

from ....garjus import Garjus


logger = logging.getLogger('dashboard.stats.data')


def get_filename():
    datadir = f'{Garjus.userdir()}/DATA'
    filename = f'{datadir}/statsdata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def run_refresh(filename, projects):
    # force a requery
    df = get_data(projects)

    save_data(df, filename)

    return df


def load_options(selected_proj=None):

    try:
        garjus = Garjus()

        if not garjus.redcap_enabled():
            return [], []

        proj_options = garjus.projects()

        proc_options = []
        if selected_proj:
            for p in selected_proj:
                proc_options.extend(garjus.stattypes(p))

        proc_options = sorted(list(set(proc_options)))

        return proj_options, proc_options
    except Exception as err:
        logger.debug(err)
        return [], []


def load_data(projects, refresh=False, filename=None):
    if not filename:
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

    if not projects:
        return df

    # Concat project stats list of stats
    assessors = garjus.assessors(projects)
    for p in sorted(projects):
        # Load stats
        stats = garjus.stats(p, assessors)
        df = pd.concat([df, stats])

    # Apply tweaks
    if 'SESSTYPE' in df.columns:
        df['SESSTYPE'] = df['SESSTYPE'].fillna('UNKNOWN')
    else:
        df['SESSTYPE'] = 'UNKNOWN'

    if 'DATE' in df:
        # Format as string to drop the time portion
        df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')

    # http link to session in xnat
    df['SESSIONLINK'] = garjus.xnat().host + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    return df


def filter_data(df, proctypes, timeframe, sesstypes):
    if df.empty:
        # It's already empty, just send it back
        return df

    logger.debug(f'applying filters:{proctypes}:{timeframe}:{sesstypes}')

    # Filter by proctype
    if proctypes:
        logger.debug(f'filtering by proctypes:{proctypes}')
        df = df[df['PROCTYPE'].isin(proctypes)]
    else:
        logging.debug('no proctypes')
        df = df[df['PROCTYPE'].isin([])]

    # Filter by timeframe
    if timeframe in ['1day', '7day', '30day', '365day']:
        logging.debug('filtering by ' + timeframe)
        then_datetime = datetime.now() - pd.to_timedelta(timeframe)
        df = df[pd.to_datetime(df.DATE) > then_datetime]
    else:
        # ALL
        logging.debug('not filtering by time')
        pass

    # Filter by sesstype
    if sesstypes:
        logging.debug(f'filtering by sesstypes:{sesstypes}')
        df = df[df['SESSTYPE'].isin(sesstypes)]
    else:
        logging.debug('no sesstypes')

    # Remove empty columns
    df = df.dropna(axis=1, how='all')

    return df
