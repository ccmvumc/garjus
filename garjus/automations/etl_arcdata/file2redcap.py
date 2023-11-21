import glob
import re
import logging
import json
import os
from datetime import datetime

from ...utils_redcap import upload_file


logger = logging.getLogger('garjus.automations.arc_file2redcap')


# first load file to determine participant_id, session, day, session_date
# drop leading zero from participant_id

# Try to match with existing record. Upload to existing record.

# match to fields in redcap:
# subj_num = participant_id
# arc_response_date =  session_date
# arc_day_index = day
# arc_order_index = id


def _load_testfile(testfile):
    data = []

    with open(testfile) as f:
        data = json.load(f)

    return data


def _subject_files(subject, files):
    subj_files = []

    for testfile in files:
        #logger.debug(f'loading testfile:{testfile}')
        d = _load_testfile(testfile)
        if d['participant_id'][1:] == subject:
            subj_files.append(testfile)

    return sorted(subj_files)


def process(project, datadir):
    file_field = 'arc_testfile'
    results = []
    def_field = project.def_field
    fields = [def_field, file_field, 'arc_response_date','arc_day_index', 'arc_order_index', 'vitals_date']
    subj2id = {}
    subjects = []
    file_glob = f'{datadir}/device_*_test_*.json'

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        subj2id = {x[sec_field]: x[def_field] for x in rec if x[sec_field]}
        subjects = sorted(list(set([x[sec_field] for x in rec if x[sec_field]])))
    else:
        rec = project.export_records(fields=[def_field])
        subj2id = {x[def_field]: x[def_field] for x in rec if x[def_field]}
        subjects = sorted(list(set([x[def_field] for x in rec])))

    # Get file records
    all_records = project.export_records(fields=fields)
    arc_records = [x for x in all_records if x['redcap_repeat_instance']]

    # Get the file list
    files = sorted(glob.glob(file_glob))

    file_count = len(files)
    if file_count <= 0:
        logger.debug(f'no files found:{file_glob}')
        return []
    else:
        logger.debug(f'found {file_count} files:{file_glob}')

    # Process each subject
    for subj in subjects:

        subj_files = _subject_files(subj, files)

        if len(subj_files) == 0:
            continue

        subj_id = subj2id[subj]
        subj_events = list(set([x['redcap_event_name'] for x in all_records if x[def_field] == subj_id]))
        subj_records = [x for x in arc_records if x[def_field] == subj_id]
        subj_uploaded = list(set([x[file_field] for x in subj_records if x[file_field]]))

        for subj_file in subj_files:
            base_file = os.path.basename(subj_file)
            test_record = None
            same_event = None

            if base_file in subj_uploaded:
                logger.debug(f'already uploaded:{subj}:{base_file}')
                continue

            # Load file data
            data = _load_testfile(subj_file)

            if data['id'] == '0':
                # Ignore the practice tests
                logger.debug(f'skipping practice test:{base_file}')
                continue

            # Get params to match from file
            arc_response_date = datetime.utcfromtimestamp(data['session_date']).strftime('%Y-%m-%d')
            arc_day_index = data['day']
            arc_order_index = data['id']

            # Find existing record
            for r in subj_records:
                if not r['arc_response_date'] or (r['arc_response_date'] != arc_response_date):
                    # wrong date
                    continue
                elif str(r['redcap_repeat_instance']) != str(arc_order_index):
                    # wrong order
                    continue
                else:
                    # matches
                    test_record = r
                    break

            if not test_record:
                # try to match with similar records date
                for r in subj_records:
                    if not r['arc_response_date']:
                        # no date cannot use
                        continue
                    elif abs((datetime.strptime(r['arc_response_date'], '%Y-%m-%d') - datetime.strptime(arc_response_date, '%Y-%m-%d')).days) > 4:
                        # wrong date
                        continue
                    elif r['vitals_date'] and abs((datetime.strptime(r['vitals_date'], '%Y-%m-%d') - datetime.strptime(arc_response_date, '%Y-%m-%d')).days) > 30:
                        continue
                    else:
                        same_event = r['redcap_event_name']
                        break

                # now match event instead of date
                for r in subj_records:
                    if str(r['redcap_event_name']) != str(same_event):
                        # wrong event
                        continue
                    elif str(r['arc_day_index']) != str(arc_day_index):
                        # wrong day
                        continue
                    elif str(r['arc_order_index']) != str(arc_order_index):
                        # wrong order
                        continue
                    else:
                        # matches
                        test_record = r
                        break

            if not test_record:
                # no match found, make a new one, but first determine event
                logger.debug(f'no record yet:{subj}:{arc_response_date}:{arc_day_index}:{arc_order_index}:{base_file}')

                event_id = None

                if same_event:
                    event_id = same_event
                #else:
                #    if 'month_24_arm_3' in subj_events:
                #        event_id = 'month_24_arm_3'
                #    elif 'month_16_arm_3' in subj_events:
                #        event_id = 'month_16_arm_3'
                #    elif 'month_8_arm_3' in subj_events:
                #        event_id = 'month_8_arm_3'
                #    elif 'baselinemonth_0_arm_3' in subj_events:
                #        event_id = 'baselinemonth_0_arm_3'

                if not event_id:
                    # no event found, cannot upload
                    continue

                test_record = {
                    def_field: subj_id,
                    'redcap_event_name': event_id,
                    'redcap_repeat_instance': arc_order_index,
                    'redcap_repeat_instrument': 'arc_data',
                    'arc_testfile': '',
                }

            record_id = test_record[def_field]
            event_id = test_record['redcap_event_name']
            repeat_id = test_record['redcap_repeat_instance']
            instrument = test_record['redcap_repeat_instrument']

            logger.debug(f'uploading:{record_id}:{event_id}:{instrument}:{repeat_id}:{base_file}')

            # Upload file to redcap
            try:
                result = upload_file(
                    project,
                    record_id,
                    file_field,
                    subj_file,
                    event_id=event_id,
                    repeat_id=repeat_id)

                logger.info(f'uploaded:{subj}:{event_id}:{repeat_id}:{base_file}')
            except (ValueError) as err:
                logger.error(f'error uploading:{base_file}:{err}')

            if not result:
                logger.error(f'upload failed:{subj}:{event_id}')
                continue

            results.append({
                'result': 'COMPLETE',
                'description': 'arc_file2redcap',
                'category': 'arc_file2redcap',
                'subject': subj,
                'event': event_id,
                'field': file_field})

    return results
