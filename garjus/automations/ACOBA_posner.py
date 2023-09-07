import io
import logging
import os
import tempfile

import numpy as np
import pandas as pd
import redcap


logger = logging.getLogger('garjus.automations.ACOBA_posner')


VALID = 'Valid'
INVALID = 'Invalid'
NOCUE = 'NoCue'
NEUTRAL = 'Neutral'

FORM_INCOMPLETE = 0
FORM_UNVERIFIED = 1
FORM_COMPLETE = 2

API_URL = 'https://redcap.vanderbilt.edu/api/'
API_KEY = ''

REDCAP_EVENTS = [
    'screening_arm_1',
    'baseline_arm_1',
]

REDCAP_FIELDS = [
    'participant_id',
    'posner_tabfile',
    'posner_complete',
    'posner_alerting_rt',
]

def read_edat(edat_path):
    _encoding = 'utf-8'
    _skiprows = 0
    first_field = 'ExperimentName'
    
    logger.info(f'edat_path={edat_path}')
    
    # Determine how many rows to skip prior to header
    with io.open(edat_path, encoding=_encoding) as _f:
        for line in _f:
            if line.startswith(first_field):
                break
            else:
                _skiprows += 1
                
    # Load Data
    df = pd.read_table(edat_path, sep='\t', encoding=_encoding, skiprows=_skiprows, header=0)
    return df


def load_edat(edat_path):
    logger.debug(f'parsing edat:{edat_path}')
    df = read_edat(edat_path)

    # Fix column names
    df.columns = df.columns.map(lambda x: x.replace('.', '_').replace(']', '_').replace('[', '_'))

    return df


def apply_columns(row):
    type_col = 'Code'
    row_type = row[type_col]

    row['onset'] = row['Target_OnsetTime']
    row['rt'] = row['Target_RT']
    row['acc'] = row['Target_ACC']

    if row_type == 'valid':
        row['trial_type'] = VALID
    elif row_type == 'invalid':
        row['trial_type'] = INVALID
    elif row_type == 'nocue':
        row['trial_type'] = NOCUE
    elif row_type == 'neutral':
        row['trial_type'] = NEUTRAL
    else:
        raise ValueError('unknown trial type')

    row['duration'] = 0

    return row


def download_file(proj, record_id, event_id, field_id, tmpdir):
    filename = os.path.join(tmpdir, record_id+'_'+event_id+'_'+field_id)
    
    logger.debug(f'downloading file:{record_id}:{event_id}:{field_id}')
    
    try:
        cont,hdr = proj.export_file(record=record_id, event=event_id, field=field_id)
        if cont == '':
            raise RedcapError
    except RedcapError:
        logger.error('downloading file')
        return None
    else:
        with open(filename, 'wb') as f:
            f.write(cont)

        return filename


def parse_posner_edat(edat_file):
    df = load_edat(edat_file)
    df = df.apply(apply_columns, axis=1)
    start_offset = df.iloc[0].Target_OnsetTime
    df['onset'] = (df['onset'] - start_offset) / 1000.0
    df = df[['trial_type', 'onset', 'duration', 'rt', 'acc']]

    # Drop Fixation rows?
    #df = df[df['trial_type'].isin(['Valid','Invalid','NoCue','Neutral'])]
    
    return df


def extract_posner(proj, record_id, event_id, file_field, tmpdir):
    data = {}

    tab_file = download_file(proj, record_id, event_id, file_field, tmpdir)

    if tab_file is None:
        return None

    df = parse_posner_edat(tab_file)

    for condition in [VALID, INVALID, NOCUE, NEUTRAL]:
        data[condition + '_ACC'] = df[(df['trial_type'] == condition)].acc.mean()
        if data[condition + '_ACC'] > 0:
            data[condition + '_RT_median'] = df[(df['trial_type'] == condition)].rt.median()
        else:
            data[condition + '_RT_median'] = np.nan

    return data


def load_posner(proj, record_id, event_id, file_field, tmpdir):

    posner_data = extract_posner(proj, record_id, event_id, file_field, tmpdir)

    if posner_data is None:
        logger.error('posner extract failed')
        return

    # Import the data
    _data = {}
    _data[proj.def_field] = record_id
    _data['redcap_event_name'] = event_id
    _data['posner_complete'] = FORM_COMPLETE    
    for condition in [VALID, INVALID, NOCUE, NEUTRAL]:
        _data['posner_' + condition.lower() + '_rt_median'] = posner_data[condition + '_RT_median']
        _data['posner_' + condition.lower() + '_acc'] = posner_data[condition + '_ACC']

    # Append additional RT measures
    _data['posner_alerting_rt'] = _data['posner_neutral_rt_median'] - _data['posner_nocue_rt_median']
    _data['posner_reorienting_rt'] = _data['posner_valid_rt_median'] - _data['posner_invalid_rt_median']

    try:
        response = proj.import_records([_data])
        assert 'count' in response
        logger.info(f'uploaded:{record_id}:{event_id}')
    except AssertionError as err:
        logger.error(f'uploaded:{record_id}:{event_id}')


logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(name)s:%(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

tmpdir = tempfile.mkdtemp()
logger.debug(f'tmpdir:{tmpdir}')

# Load the REDCap database
logger.info('INFO:loading REDCap...')
proj = redcap.Project(API_URL, API_KEY)
data = proj.export_records(events=REDCAP_EVENTS, fields=REDCAP_FIELDS)

for r in data:
    if not r['posner_tabfile']:
        logger.debug(f'no file:{r}')
        continue

    if not r['posner_tabfile'].endswith('.txt'):
        logger.debug(f'wrong format:{r}')
        continue

    if r['posner_tabfile'] == 'CONVERT_FAILED.txt':
        logger.debug(f'found CONVERT_FAILED')
        continue

    if r['posner_tabfile'] == 'MISSING_DATA.txt':
        logger.debug(f'found MISSING_DATA')
        continue

    if r['posner_alerting_rt']:
        logger.debug(f'already extracted:{r[proj.def_field]}:{r["posner_tabfile"]}')
        continue


    logger.info(f'loading:{r["posner_tabfile"]}')
    load_posner(proj, r[proj.def_field], r['redcap_event_name'], 'posner_tabfile', tmpdir)
       
logger.info('DONE!')
