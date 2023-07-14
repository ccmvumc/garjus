import logging
import tempfile
import io

import pandas as pd
import numpy as np

from ...utils_redcap import field2events, download_file


def parse_tat(filename):
    encoding = 'utf-8'
    skiprows = 0
    first_field = 'ExperimentName'

    # Determine how many rows to skip prior to header
    with io.open(filename, encoding=encoding) as f:
        for line in f:
            if line.startswith(first_field):
                break
            else:
                skiprows += 1

    # Load Data
    df = pd.read_table(
        filename, sep='\t', encoding=encoding, skiprows=skiprows, header=0)

    # First exclude the practice
    df = df[df['Procedure'] == 'TrialProc']

    # Combine respones from Target and Mask
    df = df.apply(combine_tat_responses, axis=1)

    # Drop any rows without a response
    df = df.dropna(subset=['Combined.RESP'])

    return df


def combine_tat_responses(row):
    if not pd.isnull(row['Target.RESP']):
        row['Combined.RESP'] = row['Target.RESP']
        row['Combined.RT'] = row['Target.RT']
    elif not pd.isnull(row['Mask.RESP']):
        row['Combined.RESP'] = row['Mask.RESP']
        row['Combined.RT'] = row['Mask.RT']
    else:
        row['Combined.RESP'] = np.nan
        row['Combined.RT'] = np.nan

    return row


def load_tatfile(filename):
    df = parse_tat(filename)
    data = {
        'total_tm': '0',
        'number_endorsed_bad_tm': '0',
        'number_reject_bad_tm': '0',
        'number_endorsed_good_tm': '0',
        'number_reject_good_tm': '0',
        'rt_endorsed_bad_tm': '',
        'rt_reject_bad_tm': '',
        'rt_endorsed_good_tm': '',
        'rt_reject_good_tm': ''}

    if len(df) > 0:
        data['total_tm'] = len(df)

        # Remove whitespace that we know is there
        df['EmoWord'] = df['EmoWord'].str.strip()

        # Separate good and bad words
        dfb = df[df['EmoWord'] == 'bad']
        dfg = df[df['EmoWord'] == 'good']

        if len(dfb) > 0:
            data['number_endorsed_bad_tm'] = len(dfb[dfb['Combined.RESP'] == 'm'])
            data['rt_endorsed_bad_tm'] = np.around(np.mean(dfb[dfb['Combined.RESP'] == 'm']['Combined.RT']), decimals=1)
            data['number_reject_bad_tm'] = len(dfb[dfb['Combined.RESP'] == 'x'])
            data['rt_reject_bad_tm'] = np.around(np.mean(dfb[dfb['Combined.RESP'] == 'x']['Combined.RT']), decimals=1)

        if len(dfg) > 0:
            data['number_endorsed_good_tm'] = len(dfg[dfg['Combined.RESP'] == 'm'])
            data['rt_endorsed_good_tm'] = np.around(np.mean(dfg[dfg['Combined.RESP'] == 'm']['Combined.RT']), decimals=1)
            data['number_reject_good_tm'] = len(dfg[dfg['Combined.RESP'] == 'x'])
            data['rt_reject_good_tm'] = np.around(np.mean(dfg[dfg['Combined.RESP'] == 'x']['Combined.RT']), decimals=1)

    return data


def etl_tat(project, record_id, event_id):
    data = None
    tab_file = None
    tab_field = 'tat_conv'

    with tempfile.TemporaryDirectory() as tmpdir:
        tab_file = f'{tmpdir}/{record_id}-{event_id}-{tab_field}.txt'

        # Download TAT converted tab file
        try:
            download_file(
                project,
                record_id,
                tab_field,
                tab_file,
                event_id=event_id)
        except Exception as err:
            logging.error(f'download failed:{record_id}:{event_id}:{err}')
            return False

        # Transform data
        try:
            data = _transform(tab_file)
            data[project.def_field] = record_id
            data['redcap_event_name'] = event_id
        except Exception as err:
            logging.error(f'transform failed:{record_id}:{event_id}:{err}')
            return False

    # Load the data back to redcap
    try:
        _response = project.import_records([data])
        assert 'count' in _response
        logging.info(f'TAT uploaded:{record_id}')
    except AssertionError as err:
        logging.error(f'TAT upload:{record_id}:{err}')
        return False

    return True


def _transform(filename):
    # Load the data
    logging.info('{}:{}'.format('extracting tat', filename))
    tat_data = load_tatfile(filename)
    if tat_data is None:
        logging.error('extract failed')
        return

    # Transform the data
    data = {}
    data['total_tm'] = str(tat_data['total_tm'])
    data['number_endorsed_good_tm'] = str(tat_data['number_endorsed_good_tm'])
    data['number_endorsed_bad_tm'] = str(tat_data['number_endorsed_bad_tm'])
    data['number_reject_good_tm'] = str(tat_data['number_reject_good_tm'])
    data['number_rejected_bad_tm'] = str(tat_data['number_reject_bad_tm'])
    data['rt_endorsed_good_tm'] = str(tat_data['rt_endorsed_good_tm'])
    data['rt_endorsed_bad_tm'] = str(tat_data['rt_endorsed_bad_tm'])
    data['rt_reject_good_tm'] = str(tat_data['rt_reject_good_tm'])
    data['rt_reject_bad_tm'] = str(tat_data['rt_reject_bad_tm'])

    return data


def process_project(project):
    tab_field  = 'tat_conv'
    done_field = 'total_tm'
    results = []
    def_field = project.def_field
    fields = [def_field, tab_field, done_field]
    id2subj = {}
    events = field2events(project, tab_field)
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
    else:
        rec = project.export_records(fields=[def_field])
        id2subj = {x[def_field]: x[def_field] for x in rec if x[def_field]}

    # Get records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event_id = r['redcap_event_name']
        subj = id2subj.get(record_id, record_id)

        # Check for converted file
        if not r[tab_field]:
            logging.debug(f'not yet converted:{record_id}:{subj}:{event_id}')
            continue

        # Determine if ETL has already been run
        if r[done_field]:
            logging.debug(f'already ETL:{record_id}:{subj}:{event_id}')
            continue

        # Do the ETL
        result = etl_tat(project, record_id, event_id)
        if result:
            results.append({
                'result': 'COMPLETE',
                'category': 'etl_traitadjtask',
                'description': 'etl_traitadjtask',
                'subject': subj,
                'event': event_id,
                'field': tab_field})

    return results
