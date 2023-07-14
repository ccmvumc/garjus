import logging
import tempfile
import io

import pandas as pd
import numpy as np

from ...utils_redcap import field2events, download_file


def parse_tar(filename):
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

    return df


def load_tarfile(filename):
    df = parse_tar(filename)

    # Remove whitespace that we know is there
    df['EmoWord'] = df['EmoWord'].str.strip()

    # Separate good and bad words
    dfb = df[df['EmoWord'] == 'bad']
    dfg = df[df['EmoWord'] == 'good']

    # Calculate summary data
    data = {}
    data['tar_hit_good'] = len(dfg[(dfg['RecallListB.CRESP'] == 'm') & (dfg['RecallListB.RESP'] == 'm')])
    data['tar_miss_good'] = len(dfg[(dfg['RecallListB.CRESP'] == 'm') & (dfg['RecallListB.RESP'] == 'x')])
    data['tar_fa_good'] = len(dfg[(dfg['RecallListB.CRESP'] == 'x') & (dfg['RecallListB.RESP'] == 'm')])
    data['tar_cr_good'] = len(dfg[(dfg['RecallListB.CRESP'] == 'x') & (dfg['RecallListB.RESP'] == 'x')])
    data['tar_hit_bad'] = len(dfb[(dfb['RecallListB.CRESP'] == 'm') & (dfb['RecallListB.RESP'] == 'm')])
    data['tar_miss_bad'] = len(dfb[(dfb['RecallListB.CRESP'] == 'm') & (dfb['RecallListB.RESP'] == 'x')])
    data['tar_fa_bad'] = len(dfb[(dfb['RecallListB.CRESP'] == 'x') & (dfb['RecallListB.RESP'] == 'm')])
    data['tar_cr_bad'] = len(dfb[(dfb['RecallListB.CRESP'] == 'x') & (dfb['RecallListB.RESP'] == 'x')])

    return data


def etl_tar(project, record_id, event_id):
    data = None
    tab_file = None
    tab_field = 'tat_recall_conv'

    with tempfile.TemporaryDirectory() as tmpdir:
        tab_file = f'{tmpdir}/{record_id}-{event_id}-{tab_field}.txt'

        # Download converted tab-delimited file
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
        logging.info(f'TAR uploaded:{record_id}')
    except AssertionError as err:
        logging.error(f'TAR upload:{record_id}:{err}')
        return False

    return True


def _transform(filename):
    data = {}

    # Load the data
    logging.info(f'extracting tar:{filename}')
    tar_data = load_tarfile(filename)
    if tar_data is None:
        logging.error('extract failed')
    else:
        # Transform the data and force strings for redcap
        data['recall_hit_good'] = str(tar_data['tar_hit_good'])
        data['recall_miss_good'] = str(tar_data['tar_miss_good'])
        data['recall_fa_good'] = str(tar_data['tar_fa_good'])
        data['recall_cr_good'] = str(tar_data['tar_cr_good'])
        data['recall_hit_bad'] = str(tar_data['tar_hit_bad'])
        data['recall_miss_bad'] = str(tar_data['tar_miss_bad'])
        data['recall_fa_bad'] = str(tar_data['tar_fa_bad'])
        data['recall_cr_bad'] = str(tar_data['tar_cr_bad'])

    return data


def process_project(project):
    results = []
    tab_field = 'tat_recall_conv'
    done_field = 'recall_hit_good'
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
        subj = id2subj.get(record_id)

        # Check for converted file
        if not r[tab_field]:
            logging.debug(f'not yet converted:{record_id}:{subj}:{event_id}')
            continue

        # Determine if ETL has already been run
        if r[done_field]:
            logging.debug(f'already ETL:{record_id}:{subj}:{event_id}')
            continue

        # Do the ETL
        result = etl_tar(project, record_id, event_id)
        if result:
            results.append({
                'result': 'COMPLETE',
                'category': 'etl_traitadjrecall',
                'description': 'etl_traitadjrecall',
                'subject': subj,
                'event': event_id,
                'field': tab_field})

    return results
