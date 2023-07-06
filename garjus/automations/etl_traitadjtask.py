import logging
import tempfile
import io

import pandas as pd
import numpy as np

from ...utils_redcap import field2events, download_file


tab_field = ''
done_field = 'total_tm'


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


def extract_tat(filename):
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


def etl_tat(project, record_id, event_id, tab_field):
    # Download the tab file from redcap to tmp
    tmpdir = tempfile.mkdtemp()
    basename = '{}-{}-{}.txt'.format(record_id, event_id, tab_field)
    tab_file = f'{tmpdir}/{basename}'
    result = download_file(
        project, record_id, tab_field, tab_file, event_id=event_id)
    if not result:
        logging.error(f'download failed:{record_id}:{event_id}')
        return

    # Extract the data
    logging.info('{}:{}'.format('extracting tat', tab_file))
    tat_data = extract_tat(tab_file)
    if tat_data is None:
        logging.error('extract failed')
        return

    # Transform the data
    data = {}
    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id
    data['total_tm'] = str(tat_data['total_tm'])
    data['number_endorsed_good_tm'] = str(tat_data['number_endorsed_good_tm'])
    data['number_endorsed_bad_tm'] = str(tat_data['number_endorsed_bad_tm'])
    data['number_reject_good_tm'] = str(tat_data['number_reject_good_tm'])
    data['number_rejected_bad_tm'] = str(tat_data['number_reject_bad_tm'])
    data['rt_endorsed_good_tm'] = str(tat_data['rt_endorsed_good_tm'])
    data['rt_endorsed_bad_tm'] = str(tat_data['rt_endorsed_bad_tm'])
    data['rt_reject_good_tm'] = str(tat_data['rt_reject_good_tm'])
    data['rt_reject_bad_tm'] = str(tat_data['rt_reject_bad_tm'])

    # Load the data back to redcap
    try:
        response = project.import_records([data])
        assert 'count' in response
        logging.info('{}:{}'.format('TAT uploaded', record_id))
    except AssertionError as e:
        print('ERROR:TAT upload:', record_id, e)


def process_project(project):
    results = []
    def_field = project.def_field
    fields = [def_field, tab_field, done_field]
    id2subj = {}
    events = field2events(project, tab_field)
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event = r['redcap_event_name']
        subj = id2subj.get(record_id, record_id)

        # Make visit name for logging
        visit = '{}:{}'.format(subj, event)

        # Check for converted file
        if not r[tab_field]:
            logging.debug(visit + ':not yet converted')
            continue

        # Determine if ETL has already been run
        if not done_field:
            if r['total_tm']:
                logging.debug(visit + ':already ETL')
                continue
        elif r[done_field]:
            logging.debug(visit + ':already ETL')
            continue

        # Do the ETL
        etl_tat(project, record_id, event)

        logging.debug(visit + ':uploaded')
        results.append({
            'result': 'COMPLETE',
            'type': 'ETL',
            'subject': subj,
            'event': event,
            'field': tab_field})

    return results
