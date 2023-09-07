import os
import logging
import sys
import io
import re
import datetime
import fnmatch
import tempfile
from datetime import datetime

import pandas as pd
import numpy as np
import redcap


logger = logging.getLogger('garjus.automations.ACOBA_crt')


API_URL = 'https://redcap.vanderbilt.edu/api/'
API_KEY = ''

FORM_INCOMPLETE = 0
FORM_UNVERIFIED = 1
FORM_COMPLETE = 2

REDCAP_EVENTS = ['screening_arm_1', 'baseline_arm_1']

REDCAP_FIELDS = [
    'participant_id',
    'crt_file',
    'crt_recog_mean',
    'crt_recog_median',
    'crt_motor_mean',
    'crt_motor_median',
    'crt_total_mean',
    'crt_total_median',
    'crt_notes',
    'choice_reaction_time_complete',
]


def reformat_date(date_str, in_format, out_format):
    return datetime.strptime(date_str,in_format).strftime(out_format)


def parse_crt(crt_file):
    logger.debug(f'parsing CRT:{crt_file}')
    
    xl = pd.ExcelFile(crt_file)
    _df = xl.parse(xl.sheet_names[0], header=None)
    _data = dict()
           
    if _df.iloc[0][0] != 'Choice Reaction Time Results:':
        logger.error('invalid CRT file')
        return None

    date_str = str(_df.iloc[1][0].split(' ')[1])
    _data['crt_date'] = reformat_date(date_str,'%m/%d/%Y', '%Y-%m-%d')
    _data['crt_num_trials'] = str(int(_df.iloc[2][2]))
    _data['crt_mean_recog_time'] = str(_df.iloc[6][3])
    _data['crt_mean_motor_time'] = str(_df.iloc[7][3])
    _data['crt_mean_tot_time'] = str(_df.iloc[8][3])
    _data['crt_median_recog_time'] = str(_df.iloc[6][5])
    _data['crt_median_motor_time'] = str(_df.iloc[7][5])
    _data['crt_median_tot_time'] = str(_df.iloc[8][5])
    return _data


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


def extract_crt(proj, record_id, event_id, file_field, tmpdir):
    _file = download_file(proj, record_id, event_id, file_field, tmpdir)

    if _file is None:
        return None
   
    _data = parse_crt(_file)
    
    return _data


def load_crt(proj, record_id, event_id, file_field, tmpdir):
    logger.debug('extracting:'+file_field)
    crt_data = extract_crt(proj, record_id, event_id, file_field, tmpdir)
    if crt_data is None:
        logger.error(f'extract failed:{record_id}')
        return

    # Import the data
    data = {}
    data[proj.def_field] = record_id
    data['redcap_event_name'] = event_id
    data['crt_recog_mean'] = crt_data['crt_mean_recog_time']
    data['crt_recog_median'] = crt_data['crt_median_recog_time'] 
    data['crt_motor_mean'] = crt_data['crt_mean_motor_time'] 
    data['crt_motor_median'] = crt_data['crt_median_motor_time'] 
    data['crt_total_mean'] = crt_data['crt_mean_tot_time'] 
    data['crt_total_median'] = crt_data['crt_median_tot_time'] 
    data['choice_reaction_time_complete'] = FORM_COMPLETE
    try:
        response = proj.import_records([data])
        assert 'count' in response
        logger.debug(f'CRT uploaded:{record_id}')
    except AssertionError as err:
        logger.error(f'CRT upload:{record_id}:{err}')



logging.basicConfig(
    format='%(asctime)s - %(levelname)s:%(name)s:%(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S')

tmpdir = tempfile.mkdtemp()
logger.debug(f'tmpdir:{tmpdir}')

# Load the REDCap database
logger.info('INFO:loading REDCap...')
proj = redcap.Project(API_URL, API_KEY)
data = proj.export_records(events=REDCAP_EVENTS, fields=REDCAP_FIELDS)

for r in data:
    if not r['crt_file']:
        logger.debug(f'no file:{r}')
        continue

    if not str(r['crt_file']).endswith('.xlsx'):
        logger.debug(f'wrong format:{r}')
        continue

    if r['crt_recog_mean']:
        logger.debug(f'DEBUG:already extracted:{r[proj.def_field]}:{r["crt_file"]}:{r["crt_recog_mean"]}')
        continue

    logger.info(f'loading:{r["crt_file"]}')
    load_crt(proj, r[proj.def_field], r['redcap_event_name'], 'crt_file', tmpdir)
       
logger.info('DONE!')
