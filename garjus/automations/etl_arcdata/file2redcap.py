import glob
import logging
import json
import os
from datetime import datetime

from ...utils_redcap import upload_file


logger = logging.getLogger('garjus.automations.etl_arcdata.file2redcap')


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

    if subject.startswith('3REM'):
        subject_code = '03' + subject[4:]
    else:
        subject_code = subject

    print(subject, subject_code)

    for testfile in files:
        d = _load_testfile(testfile)
        if d['participant_id'][1:] == subject_code:
            subj_files.append(testfile)

    return sorted(subj_files)


def process(project, datadir):
    file_field = 'arc_testfile'
    results = []
    def_field = project.def_field
    fields = [def_field, file_field, 'arc_response_date', 'arc_day_index', 'arc_order_index', 'vitals_date', 'date_devices_given']
    subj2id = {}
    subjects = []
    file_glob = f'{datadir}/device_*_test_*.json'
    uploaded_count = 0

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

    all_uploaded = list(set([x[file_field] for x in all_records if x[file_field]]))

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
        uploaded_count += len(subj_uploaded)

        for subj_file in subj_files:
            base_file = os.path.basename(subj_file)
            test_record = None
            same_event = None

            if base_file in all_uploaded:
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
                for r in all_records:

                    if r[def_field] != subj_id:
                        continue


                    if not (r.get('arc_response_date', False) or r.get('date_devices_given', False) or r.get('vitals_date', False)):
                        continue

                    if r.get('arc_response_date', False) and abs((datetime.strptime(r['arc_response_date'], '%Y-%m-%d') - datetime.strptime(arc_response_date, '%Y-%m-%d')).days) > 4:
                        # wrong date
                        continue

                    if r.get('date_devices_given', False):
                        date_devices_given = datetime.strptime(r['date_devices_given'], '%Y-%m-%d')
                        session_date = datetime.strptime(arc_response_date, '%Y-%m-%d')
                        diff_days = abs((date_devices_given - session_date).days)
                        if diff_days > 14:
                            continue

                    if r.get('vitals_date', False) and abs((datetime.strptime(r['vitals_date'], '%Y-%m-%d') - datetime.strptime(arc_response_date, '%Y-%m-%d')).days) > 14:
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
                    logger.debug(f'no event found:{subj}:{arc_response_date}:{arc_day_index}:{arc_order_index}:{base_file}')
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


            logger.debug(f'sanity check:{record_id}:{event_id}:{instrument}:{repeat_id}:{base_file}')
            found = False
            for x in all_records:
                if x[def_field] != subj_id or x['redcap_event_name'] != event_id or x['redcap_repeat_instrument'] != 'arc_data' or x['redcap_repeat_instance'] != repeat_id:
                    continue

                if x[file_field]:
                    logger.debug(f'found:{record_id}:{event_id}:{instrument}:{repeat_id}:{base_file}')
                    found = True

            if found:
                continue

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
                'repeat': repeat_id,
                'field': file_field})

    logger.info(f'total number uploaded={uploaded_count}')

    return results
