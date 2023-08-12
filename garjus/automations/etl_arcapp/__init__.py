"""ARC app."""
import logging
import tempfile

import pandas as pd

from ...utils_redcap import field2events, download_file


logger = logging.getLogger('garjus.automations.etl_arcapp')


def process_project(project):
    '''project is a pycap project for project that contains arcapp data'''
    results = []
    file_field = 'arcapp_csvfile'
    def_field = project.def_field
    fields = [def_field, file_field]
    id2subj = {}
    events = field2events(project, file_field)
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
        if not r[file_field]:
            logging.debug(f'no arcapp file:{record_id}:{subj}:{event_id}')
            continue

        if r[file_field] == 'CONVERT_FAILED.txt':
            logging.debug(f'found CONVERT_FAILED')
            continue

        if r[file_field] == 'MISSING_DATA.txt':
            logging.debug(f'found MISSING_DATA')
            continue

        # Do the ETL
        result = _process(project, record_id, event_id)
        if result:
            results.append({
                'result': 'COMPLETE',
                'category': 'etl_arcapp',
                'description': 'etl_arcapp',
                'subject': subj,
                'event': event_id,
                'field': file_field})

    return results

def _process(project, record_id, event_id):
    file_field = 'arcapp_csvfile'

    logger.debug(f'etl_arcapp:{record_id}:{event_id}')

    # check for existing records in the repeating instruments for this event
    rec = project.export_records(
        records=[record_id],
        events=[event_id],
        forms=['arc_data'],
        fields=[project.def_field]
    )

    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'arc_data']

    if len(rec) > 0:
        logger.debug(f'found existing records:{record_id}:{event_id}')
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_file = f'{tmpdir}/{record_id}-{event_id}-{file_field}.csv'

        logger.debug(f'downloading file:{record_id}:{event_id}')    

        try:
            download_file(
                project,
                record_id,
                file_field,
                csv_file,
                event_id=event_id)
        except Exception as err:
            logging.error(f'download failed:{record_id}:{event_id}:{err}')
            return False

        # Transform data
        try:
            data = _transform(csv_file)
            for d in data:
                d[project.def_field] = record_id
                d['redcap_event_name'] = event_id
        except Exception as err:
            logging.error(f'transform failed:{record_id}:{event_id}:{err}')
            import traceback
            traceback.print_exc()
            return False

    if not data:
        return False

    # Also complete the file form
    data.append({
        project.def_field: record_id,
        'redcap_event_name': event_id,
        'arc_app_complete': '2'
    })

    # Finally load back to redcap
    result = _load(project, data)

    return result


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False

def _read(filename):
    df = pd.DataFrame()

    try:
        # Load Data
        df = pd.read_excel(filename, dtype=str)
    except ValueError as err:
        logger.error(f'failed to read excel:{filename}:{err}')

    return df

def _transform(filename):
    data = []

    # Load the data
    logging.info(f'loading:{filename}')
    df = _read(filename)

    if df.empty:
        logging.debug(f'empty file')
        return []

    df = df.fillna('')    

    if df is None:
        logging.error('extract failed')
        return

    for i in range(1,29):
        r = None
        d = {
            'redcap_repeat_instance': str(i),
            'redcap_repeat_instrument': 'arc_data',
            'arc_order_index': str(i),
            'arc_data_complete': '2'}

        rows = df[df.id == str(i)]

        if len(rows) > 1:
            logger.error(f'multiple rows for id:{i}')
            return []
        elif len(rows) == 0:
            logger.debug(f'no rows for id:{i}')
            continue

        # Get the only record as dictionary
        r = rows.to_dict('records')[0]

        if r.get('completeTime', False):
            if 'T' in r['completeTime']:
                d['arc_completetime'] = r['completeTime'].split('T')[1].split('.')[0].rsplit(':', 1)[0]
            else:
                d['arc_completetime'] = r['completeTime'].split(' ')[1].split('.')[0].rsplit(':', 1)[0]

        if r.get('startTime', False):
            if 'T'  in r['startTime']:
                d['arc_response_date'] = r['startTime'].split('T')[0]
                d['arc_starttime'] = r['startTime'].split('T')[1].split('.')[0].rsplit(':', 1)[0]
            else:
                d['arc_response_date'] = r['startTime'].split(' ')[0]
                d['arc_starttime'] = r['startTime'].split(' ')[1].split('.')[0].rsplit(':', 1)[0]

        if r.get('prescribedTime', False):
            d['arc_notification_time'] = r['prescribedTime'].split('T')[1].split('.')[0].rsplit(':', 1)[0]

        if 'index' in r:
            d['arc_index_no'] = r['index']

        if 'dayIndex' in r:
            d['arc_day_index'] = r['dayIndex']

        if 'finishedSession' in r:
            d['arc_finishedsession'] = r['finishedSession'].lower().replace('true', '1').replace('false', '0')

        if 'interrupted' in r:
            d['arc_interrupted'] = r['interrupted'].lower().replace('true', '1').replace('false', '0')

        if 'symbolsRT' in r:
            d['arc_symbolsrt'] = r['symbolsRT']

        if 'symbolsAcc' in r:
            d['arc_symbols_accuracy'] = r['symbolsAcc']

        if 'pricesAcc' in r:
            d['arc_prices_accuracy'] = r['pricesAcc']

        if 'pricesRT' in r:
            d['arc_pricesrt'] = r['pricesRT']

        if 'gridEd' in r:
            d['arc_grided'] = r['gridEd']

        data.append(d)

    return data
