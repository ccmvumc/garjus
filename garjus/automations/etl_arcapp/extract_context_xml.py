"""Extract Context Data from old XML files"""
import json
import tempfile
import logging

import xml.etree.ElementTree as etree

from ...utils_redcap import field2events, download_file

# These files were saved to phones and an xml file exported for each event,
# Baseline, Month8, etc. The new version does not save these files, instead
# we receive a file per testing session, so file per row in the
# repeating instrument arcdata.


logger = logging.getLogger('garjus.automations.etl_arcapp.extract_context_xml')

raw_field = 'arcapp_xmlfile'

tab_field = 'arcapp_csvfile'


def process_project(project):
    results = []
    events = field2events(project, raw_field)
    def_field = project.def_field
    fields = [def_field, raw_field, tab_field]
    id2subj = {}

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if not sec_field:
        logger.error('secondary enabled, but no secondary field found')
        return

    rec = project.export_records(fields=[def_field, sec_field])
    id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event = r['redcap_event_name']

        try:
            subj = id2subj[record_id]
        except KeyError as err:
            print('record without subject number:', err)
            continue

        # Check for no raw file
        if not r[raw_field]:
            logger.debug('{}:{}:{}'.format(subj, event, 'no xml file'))
            continue

        if not r[tab_field]:
            logger.debug('{}:{}:{}'.format(subj, event, 'not converted'))
            continue

        if r[tab_field] in ['MISSING_DATA.txt', 'CONVERT_FAILED.txt']:
            logger.debug('{}:{}:{}'.format(subj, event, 'convert failed'))
            continue

        logger.info(f'extracting:{subj}:{event}')
        result = _process(project, record_id, event)

        if result:
            results.append({
                'result': 'COMPLETE',
                'category': 'etl_arcapp.extract_context_xml',
                'description': 'etl_arcapp.extract_context_xml',
                'subject': subj,
                'event': event,
                'field': raw_field})

    return results


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False


def _process(project, record_id, event_id):
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
        count = len([x['arc_context1'] for x in rec if x['arc_context1']])
        symbol_count = len(
            [x['arc_symbolsrt'] for x in rec if x['arc_symbolsrt']])

        if count > 0:
            logger.debug(f'{count=}')
            return None

        if symbol_count == 0:
            logger.debug(f'{symbol_count=}')
            return None

    # Download xml file
    with tempfile.TemporaryDirectory() as tmpdir:
        xml_file = f'{tmpdir}/{record_id}-{event_id}-{raw_field}.xml'

        logger.debug(f'downloading file:{xml_file}')

        try:
            download_file(
                project,
                record_id,
                raw_field,
                xml_file,
                event_id=event_id)
        except Exception as err:
            logging.error(f'download failed:{record_id}:{event_id}:{err}')
            return None

        # Transform data
        try:
            data = _transform(xml_file)
            for d in data:
                d[project.def_field] = record_id
                d['redcap_event_name'] = event_id
        except Exception as err:
            logging.error(f'transform failed:{record_id}:{event_id}:{err}')
            import traceback
            traceback.print_exc()
            return None

    if not data:
        return None

    # Finally load back to redcap
    result = _load(project, data)

    return result


def _transform(filename):
    data = []

    # Load the data
    logging.info(f'loading:{filename}')
    file_data = extract_data(filename)

    file_data = [x for x in file_data if x['id'] > 0]

    for d in file_data:
        context1 = d.get('context1', '')
        context2 = d.get('context2', '')
        context3 = d.get('context3', '')
        context4 = d.get('context4', '')
        context5 = d.get('context5', '')

        if context1 == context2 == context3 == context4 == context4 == context5 == '':
            continue

        data.append({
            'redcap_repeat_instance': str(d['id']),
            'redcap_repeat_instrument': 'arc_data',
            'arc_context1': context1,
            'arc_context2': context2,
            'arc_context3': context3,
            'arc_context4': context4,
            'arc_context5': context5,
        })

    return data


def process_context_survey(data):
    result = {}

    try:
        result['context1'] = data['questions'][0]['text_value']
    except (KeyError, IndexError):
        result['context1'] = ''

    try:
        result['context2'] = data['questions'][1]['text_value']
    except (KeyError, IndexError):
        result['context2'] = ''

    try:
        result['context3'] = str(data['questions'][2]['value'])
    except (KeyError, IndexError):
        result['context3'] = ''

    try:
        result['context4'] = str(data['questions'][3]['value'])
    except (KeyError, IndexError):
        result['context4'] = ''

    try:
        result['context5'] = data['questions'][4]['text_value']
    except (KeyError, IndexError):
        result['context5'] = ''

    return result


def process_session(data):
    result = {}

    # Overview
    result['id'] = data['id']

    # Check if session has been completed
    if not data['finishedSession']:
        return result

    if not data['testData']:
        return {}

    # Context Survey
    test_data = data['testData'][0]
    context_data = process_context_survey(test_data['context_survey'])
    result.update(context_data)

    # Return the complete dictionary
    return result


def extract_data(filename):
    data = []
    jsondata = None

    root = etree.parse(filename).getroot()
    for node in root:
        if node.attrib['name'] == 'ParticipantState':
            _json = json.loads(node.text)
            jsondata = _json['testCycles'][0]['days']
            break

    # Process each day/session
    for day in jsondata:
        for sess in day['sessions']:
            sess_data = process_session(sess)
            if not sess_data:
                continue

            # Keep it
            data.append(sess_data)

    # Return the complete data
    return data
