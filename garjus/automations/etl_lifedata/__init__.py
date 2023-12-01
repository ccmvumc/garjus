"""Life Data."""
import logging
import tempfile

import pandas as pd

from ...utils_redcap import field2events, download_file


logger = logging.getLogger('garjus.automations.etl_lifedata')


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


SUMS = {
    'lifedata_deptot' : ['lifedata_worthless', 'lifedata_helpless', 'lifedata_depressed', 'lifedata_hopeless'],
    'lifedata_fatiguetot': ['lifedata_tired', 'lifedata_stress'],
    'lifedata_rumtot': ['lifedata_deserve', 'lifedata_react', 'lifedata_situation', 'lifedata_problems', 'lifedata_handle'],
    'lifedata_postot': ['lifedata_happy', 'lifedata_cheerful', 'lifedata_satisfied'],
    'lifedata_negtot': ['lifedata_down', 'lifedata_guilty', 'lifedata_lonely', 'lifedata_anxious']
}


logger = logging.getLogger('garjus.automations.etl_lifedata')


def sum_responses(data, labels):
    response_sum = 0
    for r in labels:
        if r not in data:
            return ''

        response_sum += int(data[r])

    return response_sum


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

        #if record_id != '3':
        #    continue

        # Check for converted file
        if not r[file_field]:
            logger.debug(f'no life file:{record_id}:{subj}:{event_id}')
            continue

        if r[file_field] == 'CONVERT_FAILED.txt':
            logger.debug(f'found CONVERT_FAILED')
            continue

        if r[file_field] == 'MISSING_DATA.txt':
            logger.debug(f'found MISSING_DATA')
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
            logger.error(f'download failed:{record_id}:{event_id}:{err}')
            return False

        # Transform data
        try:
            data = _transform(csv_file)
            for d in data:
                d[project.def_field] = record_id
                d['redcap_event_name'] = event_id
        except Exception as err:
            logger.error(f'transform failed:{record_id}:{event_id}:{err}')
            import traceback
            traceback.print_exc()
            return False

    if not data:
        return False

    # Also complete the lifedata file form
    data.append({
        project.def_field: record_id,
        'redcap_event_name': event_id,
        'life_data_complete': '2'
    })

    # Finally load back to redcap
    result = _load(project, data)

    return result


def _load(project, data):
    # Load the data back to redcap, data list is dictionaries
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        return False

def _read(filename):
    df = pd.DataFrame()

    try:
        # Load Data
        df = pd.read_csv(filename, dtype=str)
    except ValueError as err:
        logger.error(f'failed to read excel:{filename}:{err}')

    return df


def get_response(df, label):
    if len(df[df['Prompt Label'] == label]) > 1:
        logger.debug('duplicates!')
    elif len(df[df['Prompt Label'] == label]) == 0:
        logger.debug(f'missing:{label}')
        return ''

    return df[df['Prompt Label'] == label].iloc[0].Response


def _transform(filename):
    data = []

    # Load the data
    logger.info(f'loading:{filename}')
    df = _read(filename)

    if df.empty:
        logger.debug(f'empty file')
        return []

    df = df.fillna('')

    if df is None:
        logger.error('extract failed')
        return

    # make a record per notification
    for i in df['Notification No'].unique():
        d = {
            'redcap_repeat_instance': str(i),
            'redcap_repeat_instrument': 'ema_lifedata_survey',
            'ema_lifedata_survey_complete': '2'}

        dfs = df[df['Notification No'] == i]

        if dfs.empty:
            logger.debug(f'no rows for Notification No:{i}')
            continue

        d['lifedata_notification_time'] = dfs.iloc[0]['Notification Time']
        d['lifedata_notification_no'] = dfs.iloc[0]['Notification No']

        if dfs.iloc[0]['Session Instance No']:
            d['lifedata_session_no'] = str(int(float(
                dfs.iloc[0]['Session Instance No'])))

        if dfs.iloc[0]['Responded'] != '1':
            d['lifedata_responded'] = '0'
        else:
            d['lifedata_responded'] = '1'

            if dfs.iloc[0]['Prompt Response Time']:
                # Get the response date and time separately
                d['lifedata_response_date'] = dfs.iloc[0]['Prompt Response Time'].split(' ')[0]
                d['lifedata_response_time'] = dfs.iloc[0]['Prompt Response Time'].split(' ')[1].rsplit(':', 1)[0]

                # Get the prompt values
                for k, v in VARMAP.items():
                    response = get_response(dfs, v)
                    if response:
                        d[k] = str(int(float(response)))

            if dfs.iloc[0]['Session Length']:
                d['lifedata_session_length'] = dfs.iloc[0]['Session Length'].split(':', 1)[1]

            # Calculate sums for this session
            for k, v in SUMS.items():
                # Creates a new value for key k in record d which sums
                # the values from the list of keys k
                d[k] = sum_responses(d, v)

        # Append to our list of records
        data.append(d)

    return data
