import logging
import os

import pandas as pd

from ...garjus import Garjus
from ..utils import file_age


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
    g = Garjus()

    logger.info('loading issues')
    df = g.issues()

    # Sort by date and reset index
    df.sort_values(by=['DATETIME'], inplace=True, ascending=False)
    df.reset_index(inplace=True)

    df['ID'] = df.index
    df['STATUS'] = 'FAIL'
    df['LABEL'] = df['ID']

    df['SESSIONLINK'] = g.xnat_host() + \
        '/data/projects/' + df['PROJECT'] + \
        '/subjects/' + df['SUBJECT'] + \
        '/experiments/' + df['SESSION']

    project2id = {}

    for p in df.PROJECT.unique():
        project_id = g.project_setting(p, 'primary')
        project2id[p] = project_id

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
            df.loc[df['PROJECT'] == p, 'SUBJECTID'] = df['SUBJECT'].map(subj2id)
        else:
            # ID is same as subject number for this project
            pass

    # TODO: make event link
    #https://redcap.vanderbilt.edu/redcap_v13.9.3/DataEntry/index.php?\
    #pid=X&page=mri_scan_form&id=Y&event_id=Z
 
    # Make project link
    df['PROJECTLINK'] = 'https://redcap.vanderbilt.edu/redcap_v13.9.3/' + \
        'DataEntry/record_home.php?pid=' + df['PROJECTPID']

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
