"""ARC app."""
import logging
import tempfile
import json
import math
from datetime import datetime

import pandas as pd

from ...utils_redcap import download_named_file
from . import file2redcap
from . import summary


logger = logging.getLogger('garjus.automations.etl_arcdata')


file_field = 'arc_testfile'


def process(project, datadir):
    results = []
    def_field = project.def_field
    fields = [def_field, file_field, 'arc_missedsession']

    logger.debug('file2redcap')
    results = file2redcap.process(project, datadir)

    logger.debug('process')

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
    else:
        rec = project.export_records(fields=[def_field])
        id2subj = {x[def_field]: x[def_field] for x in rec if x[def_field]}

    # Get file records
    logger.debug('getting arc testfile records')
    records = project.export_records(fields=fields)
    records = [x for x in records if x['redcap_repeat_instance']]
    records = [x for x in records if x[file_field]]

    logger.debug('processing arc testfile records')

    for r in records:

        record_id = r[def_field]
        event_id = r['redcap_event_name']
        repeat_id = r['redcap_repeat_instance']

        if event_id.startswith('unscheduledad_hoc_arm_3') or event_id.startswith('screening'):
            logger.debug(f'skipping:{record_id}:{event_id}:{repeat_id}')
            continue

        if r['arc_missedsession']:
            logger.debug(f'already extracted:{record_id}:{event_id}:{repeat_id}')
            continue

        # Process record
        logger.info(f'process:{r[file_field]}:{record_id}:{event_id}:{repeat_id}')
        result = _process(project, record_id, event_id, repeat_id)

        if result:
            results.append({
                'result': 'COMPLETE',
                'category': 'etl_arcdata',
                'description': 'etl_arcdata',
                'subject': id2subj[record_id],
                'event': event_id,
                'repeat': repeat_id,
                'field': file_field})

    logger.debug('summary')
    results = results + summary.process(project)

    return results


def _process(project, record_id, event_id, repeat_id):
    test_file = None
    result = []
    data = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.debug(f'downloading file:{record_id}:{event_id}:{repeat_id}') 

        try:
            test_file = download_named_file(
                project,
                record_id,
                file_field,
                tmpdir,
                event_id=event_id,
                repeat_id=repeat_id
            )
        except Exception as err:
            logging.error(f'download failed:{record_id}:{event_id}:{repeat_id}:{err}')
            return False

        logger.debug(f'transforming:{record_id}:{event_id}:{repeat_id}') 
        try:
            data = _transform(test_file)
        except Exception as err:
            logging.error(f'transform failed:{record_id}:{event_id}:{repeat_id}:{err}')
            import traceback
            traceback.print_exc()
            return False

    #if 'arc_context1' not in data:
    #    logging.error(f'nothing found:{record_id}:{event_id}:{repeat_id}')
    #    return False

    data[project.def_field] = record_id
    data['redcap_event_name'] = event_id
    data['redcap_repeat_instrument'] = 'arc_data'
    data['redcap_repeat_instance'] = repeat_id

    # Finally load back to redcap
    result = _load(project, [data])

    return result


def _process_context_survey(data):

    try:
        return {
            'arc_context1': data['questions'][0]['text_value'],
            'arc_context2': data['questions'][1]['text_value'],
            'arc_context3': str(data['questions'][2]['value']),
            'arc_context4': str(data['questions'][3]['value']),
            'arc_context5': data['questions'][4]['text_value'],
        }
    except (KeyError, IndexError):
        return {}


def _process_price_test(data):
    df = pd.DataFrame(data['sections'])

    if 'selection_time' not in df.columns:
        return {}

    # Calculate accuracy
    df['acc'] = (df['selected_index'] == df['correct_index'])
    price_acc = (df['acc'].mean() * 100).round().astype(int)

    # Calculate RT with correct trials
    df['rt'] = df['selection_time'] - df['stimulus_display_time']
    price_rt = df[df['acc'] == 1]['rt'].median()

    return {
        'arc_prices_accuracy': str(price_acc),
        'arc_pricesrt': str(round(price_rt, 3))
    }


def _process_symbol_test(data):
    df = pd.DataFrame(data['sections'])

    if 'selection_time' not in df.columns:
        return {}

    # Calculate accuracy
    df['acc'] = (df['selected'] == df['correct'])
    symbol_acc = (df['acc'].mean() * 100).round().astype(int)

    # Calculate RT with correct trials
    df['rt'] = df['selection_time'] - df['appearance_time']
    symbol_rt = df[df['acc'] == 1]['rt'].median()
    return {
        'arc_symbols_accuracy': str(symbol_acc),
        'arc_symbolsrt': str(round(symbol_rt, 3)),
    }


def _process_grid_test(data):
    ed_sec = []
    for s, cur_section in enumerate(data['sections']):
        if len(cur_section['choices']) > 0:
            for c, cur_choice in enumerate(cur_section['choices']):
                cur_ed = [None] * len(cur_section['images'])
                for i, cur_image in enumerate(cur_section['images']):
                    ix = cur_image['x']
                    iy = cur_image['y']
                    cx = cur_choice['x']
                    cy = cur_choice['y']
                    cur_ed[i] = math.sqrt(
                        ((cx - ix) * (cx - ix)) + ((cy - iy) * (cy - iy))
                    )

            # Keep running sum of Mininum ED for each choice
            ed_sec.append(min(cur_ed))
        else:
            logger.debug('empty choices')

    if not ed_sec:
        return {}
    else:
        # Average ED of sections
        grid_ed = sum(ed_sec) / float(len(ed_sec))
        return {
            'arc_grided': str(round(grid_ed, 3))
        }


def _load_testfile(testfile):
    data = []

    with open(testfile) as f:
        data = json.load(f)

    return data


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False


def _transform(filename):
    data = {}
    file_data = _load_testfile(filename)

    repeat_id = str(file_data['id'])
    data = {
        'redcap_repeat_instance': repeat_id,
        'redcap_repeat_instrument': 'arc_data',
        'arc_order_index': repeat_id,
        'arc_day_index': str(file_data['day']),
        'arc_data_complete': '2',
        'arc_interrupted': str(file_data['interrupted']),
        'arc_response_date': datetime.utcfromtimestamp(
            file_data['session_date']).strftime('%Y-%m-%d'),
        'arc_notification_time': datetime.fromtimestamp(
            file_data['session_date']).strftime('%H:%M'),
        'arc_finishedsession': str(file_data['finished_session']),
        'arc_missedsession': str(file_data['missed_session']),
        'arc_index_no': str(file_data['session']),
        'arc_session_no': str(file_data['session_id']),
    }

    if 'start_time' in file_data:
        data['arc_starttime'] = datetime.fromtimestamp(
            file_data['start_time']).strftime('%H:%M')

    if len(file_data.get('tests', 0)) > 0:
        test_data = file_data['tests'][0]

        if test_data.get('context_survey', False):
            data.update(_process_context_survey(test_data['context_survey']))

        if test_data.get('symbol_test', False):
            data.update(_process_symbol_test(test_data['symbol_test']))

        if test_data.get('price_test', False):
            data.update(_process_price_test(test_data['price_test']))

        if test_data.get('grid_test', False):
            data.update(_process_grid_test(test_data['grid_test']))

    return data
