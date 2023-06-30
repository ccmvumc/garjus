import logging


logger = logging.getLogger('garjus.issues.audit_edat')


def audit(
    project,
    events,
    raw_field,
    tab_field,
    ready_field,
):
    results = []
    id2subj = {}
    def_field = project.def_field
    fields = [def_field, raw_field, tab_field, ready_field]

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get records
    records = project.export_records(fields=fields, events=events)

    # Process each record
    for rec in records:
        record_id = rec[def_field]
        event = rec['redcap_event_name']
        subj = id2subj.get(record_id, record_id)

        # Skip if not ready
        if not rec[ready_field]:
            logger.debug(f'{subj}:{event}:not ready yet')
            continue

        # Check for edat file
        if not rec[raw_field]:
            # Missing edat
            logger.debug(f'{subj}:{event}:missing edat')
            results.append({
                'category': 'MISSING_EDAT',
                'subject': subj,
                'event': event,
                'field': raw_field,
                'description': 'need to run edat2tab or manually upload'})
            continue

        # Check for missing data flag
        if 'MISSING_DATA' in rec[raw_field]:
            logger.debug('{}:{}:{}'.format(subj, event, 'missing data flag'))
            continue

        # Check for converted edat file
        if not rec[tab_field]:
            # Missing converted edat
            logger.debug(f'{subj}:{event}:missing converted edat')
            results.append({
                'category': 'MISSING_CONVERTED_EDAT',
                'subject': subj,
                'event': event,
                'field': tab_field,
                'description': 'need to check converter vm'})
            continue

    return results
