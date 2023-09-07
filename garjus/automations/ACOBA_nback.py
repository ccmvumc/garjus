import io
import logging
import os
import tempfile

import numpy as np
import pandas as pd
import redcap


logger = logging.getLogger('garjus.automations.ACOBA_nback')


API_URL = 'https://redcap.vanderbilt.edu/api/'
API_KEY = ''

REDCAP_EVENTS = [
    'screening_arm_1',
]

REDCAP_FIELDS = [
    'participant_id',
    'nback_tabfile',
    'nback_e90d1a_complete',
    'zero_back_fa',
]

M_COUNT = 14.0

X_COUNT = 40.0


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


def parse_nback(nback_file):
    data = {}

    logger.info(f'parsing NBACK:{nback_file}')

    # Determine number of lines to skip
    skip_count = 0
    with open(nback_file) as f:
        for line in f:
            if line.startswith('ExperimentName'):
                break
            else:
                skip_count += 1

    # Read the file
    df = pd.io.parsers.read_csv(
        nback_file,
        skiprows=skip_count,
        header=0,
        index_col=False,
        sep='\t',
    )
    
    # Fix column names
    cols = df.columns
    cols = cols.map(lambda x: x.replace('.', '_').replace(']','_').replace('[','_'))
    df.columns = cols
    
    # Get ID fields 
    subj_list = df['Subject'].unique()
    if len(subj_list) > 1:
        return
    
    sess_list = df['Session'].unique()
    if len(sess_list) > 1:
        return
    
    group_list = df['Group'].unique()
    if len(group_list) > 1:
        return
    
    date_list = df['SessionDate'].unique()
    if len(date_list) > 1:
        return
    
    data['Subject'] = subj_list[0]
    data['Session'] = int(sess_list[0])
    data['Group'] = int(group_list[0])
    data['SessionDate'] = date_list[0]
    
    # Calculate counts
    hit_count_0 = len(df[(df['procedure_Block_'] == 'zeroproc')
                         & (df['LetterM_CRESP'] == 'x') & (df['LetterM_RESP'] == 'm')].index)
    hit_count_1 = len(df[(df['procedure_Block_'] == 'oneproc')
                         & (df['OneBack1_CRESP'] == 'x') & (df['OneBack1_RESP'] == 'm')].index)
    hit_count_2 = len(df[(df['procedure_Block_'] == 'twoproc')
                         & (df['TwoBack1_CRESP'] == 'x') & (df['TwoBack1_RESP'] == 'm')].index)
    hit_count_3 = len(df[(df['procedure_Block_'] == 'threeproc')
                         & (df['ThreeBack1_CRESP'] == 'x') & (df['ThreeBack1_RESP'] == 'm')].index)
    fa_count_0  = len(df[(df['procedure_Block_'] == 'zeroproc')
                         & (df['LetterM_CRESP']  == 'm') & (df['LetterM_RESP'] == 'm')].index)
    fa_count_1  = len(df[(df['procedure_Block_'] == 'oneproc')
                         & (df['OneBack1_CRESP'] == 'm') & (df['OneBack1_RESP'] == 'm')].index)
    fa_count_2  = len(df[(df['procedure_Block_'] == 'twoproc')
                         & (df['TwoBack1_CRESP'] == 'm') & (df['TwoBack1_RESP'] == 'm')].index)
    fa_count_3  = len(df[(df['procedure_Block_'] == 'threeproc')
                         & (df['ThreeBack1_CRESP'] == 'm') & (df['ThreeBack1_RESP'] == 'm')].index)
    
    # Calculate ratios of hits and false alarms
    data['hit_ratio_0'] = hit_count_0/M_COUNT
    data['hit_ratio_1'] = hit_count_1/M_COUNT
    data['hit_ratio_2'] = hit_count_2/M_COUNT
    data['hit_ratio_3'] = hit_count_3/M_COUNT
    data['fa_ratio_0'] = fa_count_0/X_COUNT
    data['fa_ratio_1'] = fa_count_1/X_COUNT
    data['fa_ratio_2'] = fa_count_2/X_COUNT
    data['fa_ratio_3'] = fa_count_3/X_COUNT
    
    return data


def extract_nback(proj, record_id, event_id, file_field, tmpdir):
    data = {}

    tab_file = download_file(proj, record_id, event_id, file_field, tmpdir)

    if tab_file is None:
        return None

    data = parse_nback(tab_file)

    return data


def load_nback(proj, record_id, event_id, file_field, tmpdir):
    nback_data = extract_nback(proj, record_id, event_id, file_field, tmpdir)

    if nback_data is None:
        logger.error('nback extract failed')
        return

    # Import the data
    data = {}
    data[proj.def_field] = record_id
    data['redcap_event_name'] = event_id
    data['nback_e90d1a_complete'] = '2'

    data['session'] = nback_data['Session']
    data['group'] = nback_data['Group']

    data['zero_back_fa'] = f'{nback_data["fa_ratio_0"]:.3f}'
    data['zero_back_hits'] = f'{nback_data["hit_ratio_0"]:.3f}'
    data['one_back_fa'] = f'{nback_data["fa_ratio_1"]:.3f}'
    data['one_back_hits'] = f'{nback_data["hit_ratio_1"]:.3f}'
    data['two_back_fa'] = f'{nback_data["fa_ratio_2"]:.3f}'
    data['two_back_hits'] = f'{nback_data["hit_ratio_2"]:.3f}'
    data['three_back_fa'] = f'{nback_data["fa_ratio_3"]:.3f}'
    data['three_back_hits'] = f'{nback_data["hit_ratio_3"]:.3f}'

    try:
        response = proj.import_records([data])
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
    if not r['nback_tabfile']:
        logger.debug(f'no file:{r}')
        continue

    if not r['nback_tabfile'].endswith('.txt'):
        logger.debug(f'wrong format:{r}')
        continue

    if r['zero_back_fa']:
        logger.debug(f'already extracted:{r[proj.def_field]}:{r["nback_tabfile"]}')
        continue

    logger.info(f'loading:{r["nback_tabfile"]}')
    load_nback(proj, r[proj.def_field], r['redcap_event_name'], 'nback_tabfile', tmpdir)
       
logger.info('DONE!')
