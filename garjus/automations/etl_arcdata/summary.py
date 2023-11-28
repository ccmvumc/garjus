import logging
from datetime import datetime


logger = logging.getLogger('garjus.automations.etl_arcdata.summary')


def _load(project, data):
    # Load the data back to redcap
    try:
        _response = project.import_records(data)
        assert 'count' in _response
        return True
    except (AssertionError, Exception) as err:
        logger.error(err)
        return False


def process(project):
    results = []
    def_field = project.def_field
    fields = [def_field, 'arcapp_numcomplete', 'arc_finishedsession', 'arc_response_date']
    subj2id = {}
    subjects = []

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

    # Get all records, repeats and non-repeats
    all_records = project.export_records(fields=fields)

    # Get the arcdata repeating records
    arc_records = [x for x in all_records if x['redcap_repeat_instance']]

    # Process each subject
    for subj in subjects:
        subj_id = subj2id[subj]
        subj_events = list(set([x['redcap_event_name'] for x in all_records if x[def_field] == subj_id]))
        subj_arc = [x for x in arc_records if x[def_field] == subj_id]

        # Iterate subject events
        for event_id in subj_events:

            # Find existing numcomplete
            numcomplete = [x for x in all_records if (x[def_field] == subj_id) and (x['redcap_event_name'] == event_id) and (x.get('arcapp_numcomplete', False))]
            if len(numcomplete) > 0:
                # numcomplete already set
                continue

            # Get repeat records
            repeats = [x for x in subj_arc if (x['redcap_event_name'] == event_id) and (x.get('arc_response_date', False))]
            if len(repeats) == 0:
                # no repeats to count
                continue

            # Check date of tests, if less than week since starting, skip
            first_date = sorted([x['arc_response_date'] for x in repeats])[0]
            if (datetime.today() - datetime.strptime(first_date, '%Y-%m-%d')).days <  7:
                logger.debug(f'SKIP:{subj}:{event_id}:{first_date}')
                continue

            finished = [x for x in subj_arc if (x['redcap_event_name'] == event_id) and (x.get('arc_finishedsession', False) == '1')]
            count_finished = len(finished)

            logger.debug(f'setting numcomplete:{subj}:{event_id}:{first_date}:{count_finished}')

            # set numcomplete equal to count_finished for record/event
            data = {
                def_field: subj_id,
                'redcap_event_name': event_id,
                'arcapp_numcomplete': str(count_finished),
                'arc_app_complete': '2',
            }
            logger.debug(f'loading numcomplete:{subj_id}:{event_id}')

            _load(project, [data])

            results.append({
                'result': 'COMPLETE',
                'description': 'arc_summary',
                'category': 'arc_summary',
                'subject': subj_id,
                'event': event_id,
                'field': 'arcapp_numcomplete'})

    return results
