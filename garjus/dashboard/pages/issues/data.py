import logging
import os

import pandas as pd

from ....garjus import Garjus
from ..utils import file_age


logger = logging.getLogger('dashboard.issues.data')


# This is where we save our cache of the data
def get_filename():
    datadir = f'{Garjus.userdir()}/DATA'
    filename = f'{datadir}/issuesdata.pkl'

    try:
        os.makedirs(datadir)
    except FileExistsError:
        pass

    return filename


def get_data():
    g = Garjus()

    if not g.redcap_enabled():
        logger.debug('redcap not enabled, no issues data')
        return pd.DataFrame(columns=g.column_names('issues'))

    pid = g.redcap_pid()

    logger.info('loading issues')
    df = g.issues()

    # Sort by date and reset index
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)

    df['STATUS'] = 'FAIL'
    df['LABEL'] = df['ID']

    df['SESSIONLINK'] = g.xnat_host()
    # + \
    #    '/data/projects/' + df['PROJECT'] + \
    #    '/subjects/' + df['SUBJECT'] + \
    #    '/experiments/' + df['SESSION']

    project2id = g.projects_setting(list(df.PROJECT.unique()), 'primary')

    df['PROJECTPID'] = df['PROJECT'].map(project2id)

    df['SUBJECTID'] = df['SUBJECT']

    # Load record IDs so we can link to the subject
    for p in df.PROJECT.unique():
        primary = g.primary(p)

        if not primary:
            logger.debug(f'no primary found:{p}')
            continue

        def_field = primary.def_field
        sec_field = primary.export_project_info()['secondary_unique_field']
        if sec_field:
            # Handle secondary ID
            rec = primary.export_records(fields=[def_field, sec_field])
            subj2id = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}
            df.loc[df['PROJECT'] == p, 'SUBJECTID'] = df['SUBJECT'].map(
                subj2id)
        else:
            # ID is same as subject number for this project
            pass

    # Make project link
    df['PROJECTLINK'] = 'https://redcap.vanderbilt.edu/redcap_v14.2.2/' + \
        '/index.php?pid=' + df['PROJECTPID']

    # Make record link
    df['IDLINK'] = 'https://redcap.vanderbilt.edu/redcap_v14.2.2/' + \
        'DataEntry/index.php?' + \
        'pid=' + str(pid) + \
        '&page=issues&id=' + \
        df['PROJECT'] + \
        '&instance=' + \
        df['ID'].astype(str)

    # TODO: not able to work yet, need more redcap ids to get to page
    #  Make field link
    df['FIELDLINK'] = 'https://redcap.vanderbilt.edu/redcap_v14.2.2/' + \
        '/index.php?pid=' + df['PROJECTPID']

    return df


def run_refresh():
    filename = get_filename()

    df = get_data()

    if not df.empty:
        save_data(df, filename)

    return df


def load_data(refresh=False, maxmins=5):
    filename = get_filename()

    if not os.path.exists(filename):
        refresh = True
    elif file_age(filename) > maxmins:
        logger.info(f'refreshing, file age limit reached:{maxmins} minutes')
        refresh = True

    if refresh:
        df = run_refresh()
    else:
        df = read_data(filename)

    return df


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
