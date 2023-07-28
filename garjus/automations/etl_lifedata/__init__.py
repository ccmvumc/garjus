"""Life Data."""
import logging
import os
import tempfile

import pandas as pd

from ...utils_redcap import field2events, download_file


VARMAP = {
    'lifedata_others': 'Basic Info (2) (2)',
    'lifedata_worthless': 'Dep 1 (2) (2)',
    'lifedata_helpless': 'Dep 2 (2) (2)',
    'lifedata_depressed': 'Dep 3 (2) (2)',
    'lifedata_hopeless': 'Dep 4 (2) (2)',
    'lifedata_fatigued': 'Fatigue 1 (2) (2)',
    'lifedata_tired': 'Fatigue 2 (2) (2)',
    'lifedata_stress': 'Stress 1 (2) (2)',
    'lifedata_work': 'Stress 2 (2) (2)_Work',
    'lifedata_family': 'Stress 2 (2) (2)_Family',
    'lifedata_financial': 'Stress 2 (2) (2)_Financial',
    'lifedata_health': 'Stress 2 (2) (2)_Health',
    'lifedata_social': 'Stress 2 (2) (2)_Social (Non-family)',
    'lifedata_other': 'Stress 2 (2) (2)_Other',
    'lifedata_nostress': 'Stress 2 (2) (2)_No Stress',
    'lifedata_deserve': 'Rum 1 (2) (2)',
    'lifedata_react': 'Rum 2 (2) (2)',
    'lifedata_situation': 'Rum 3 (2) (2)',
    'lifedata_problems': 'Rum 4 (2) (2)',
    'lifedata_handle': 'Rum 5 (2) (2)',
    'lifedata_down': 'Pos/Neg 1 (2) (2)',
    'lifedata_happy': 'Pos/Neg 5 (2) (2)',
    'lifedata_guilty': 'Pos/Neg 2 (2) (2)',
    'lifedata_cheerful': 'Pos/Neg 6 (2) (2)',
    'lifedata_lonely': 'Pos/Neg 3 (2) (2)',
    'lifedata_satisfied': 'Pos/Neg 7 (2) (2)',
    'lifedata_anxious': 'Pos/Neg 4 (2) (2)',
}


logger = logging.getLogger('garjus.automations.etl_lifedata')


# TODO: function to check for correct config of repeating instruments


def process_project(project):
    '''project is a pycap project for project that contains life data.'''
    file_field = 'life_file'
    results = []
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
            logging.debug(f'no life file:{record_id}:{subj}:{event_id}')
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
                'category': 'etl_lifedata',
                'description': 'etl_lifedata',
                'subject': subj,
                'event': event_id,
                'field': file_field})

    return results


def _process(project, record_id, event_id):
    file_field = 'life_file'

    logger.debug(f'etl_lifedata:{record_id}:{event_id}')

    # check for existing records in the repeating instruments for this event
    rec = project.export_records(
        records=[record_id],
        events=[event_id],
        forms=['ema_lifedata_survey'],
        fields=[project.def_field]
    )

    rec = [x for x in rec if x['redcap_repeat_instrument'] == 'ema_lifedata_survey']

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
        return

    # Load the data back to redcap
    try:
        logging.info(f'uploading:{record_id}')
        _response = project.import_records(data)
        assert 'count' in _response
        logging.info(f'uploaded:{record_id}')
    except AssertionError as err:
        logging.error(f'uploading:{record_id}:{err}')
        return False

    return True


def _loadfile(filename):
    df = pd.DataFrame()

    try:
        # Load Data
        df = pd.read_csv(filename, dtype=str)
    except ValueError as err:
        logger.error(f'failed to read excel:{filename}:{err}')

    return df


def get_response(df, label):
    if len(df[df['Prompt Label'] == label]) > 1:
        print('duplicates!')
    elif len(df[df['Prompt Label'] == label]) == 0:
        print('missing')

    return df[df['Prompt Label'] == label].iloc[0].Response


def _transform(filename):
    data = []

    # Load the data
    logging.info(f'loading:{filename}')
    df = _loadfile(filename)

    if df.empty:
        logging.debug(f'empty file')
        return []

    df = df.fillna('')

    if df is None:
        logging.error('extract failed')
        return

    for i in range(1,29):
        d = {
            'redcap_repeat_instance': str(i),
            'redcap_repeat_instrument': 'ema_lifedata_survey',
            'lifedata_session_no': str(i),
            'ema_lifedata_survey_complete': '2'}

        dfs = df[df['Session Instance No'] == str(float(i))]

        if dfs.empty:
            logger.debug(f'no rows for session:{i}')
            continue

        d['lifedata_notification_time'] = dfs.iloc[0]['Notification Time']
        d['lifedata_notification_no'] = dfs.iloc[0]['Notification No']

        if dfs.iloc[0]['Responded'] != '1':
            d['lifedata_responded'] = '0'
        else:
            d['lifedata_responded'] = '1'

            if dfs.iloc[0]['Prompt Response Time']:
                d['lifedata_session_length'] = dfs.iloc[0]['Session Length'].split(':', 1)[1]
                d['lifedata_response_date'] = dfs.iloc[0]['Prompt Response Time'].split(' ')[0]
                d['lifedata_response_time'] = dfs.iloc[0]['Prompt Response Time'].split(' ')[1].rsplit(':', 1)[0]

                # Get the prompt values
                for k, v in VARMAP.items():
                    d[k] = str(int(float(get_response(dfs, v))))

        # Append to our list of records
        data.append(d)

    return data
