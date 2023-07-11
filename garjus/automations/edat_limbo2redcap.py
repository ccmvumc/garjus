import glob
import re
import logging

from ..utils_redcap import upload_file


logger = logging.getLogger('garjus.automations.edat_limbo2redcap')


def process_project(
    project,
    events,
    raw_field,
    tab_field,
    edat_prefix,
    limbo_dir,
    event2sess=None,
):
    results = []
    def_field = project.def_field
    fields = [def_field, raw_field, tab_field]
    id2subj = {}

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
    else:
        rec = project.export_records(fields=[def_field])
        id2subj = {x[def_field]: x[def_field] for x in rec if x[def_field]}

    # Get mri records
    rec = project.export_records(fields=fields, events=events)

    # Process each record
    for r in rec:
        record_id = r[def_field]
        event = r['redcap_event_name']
        sess_num = '1'

        if event2sess is not None:
            sess_num = event2sess[event]

        try:
            subj = id2subj[record_id]
            logger.debug(f'{subj}:{event} using secondary id:{record_id}')
        except KeyError as err:
            logger.debug(f'record without subject number:{err}')
            continue

        if not subj:
            logger.warn(f'blank subject number:{record_id}')
            continue

        # Check for no raw file
        if r[raw_field]:
            logger.debug(f'already uploaded:{subj}:{event}')
            continue

        # Check for existing converted file
        if r[tab_field]:
            logger.debug(f'already converted:{subj}:{event}')
            continue

        # Find a file for this record
        logger.debug(f'looking for files:{subj}:{event}')

        # Get just the numeric portion of the subject number
        subj_num = re.sub(r'[^0-9]', '', subj)

        # Find files for this subject
        sess_glob = f'{limbo_dir}/{edat_prefix}-{subj_num}-{sess_num}.edat?'
        file_list = sorted(glob.glob(sess_glob))
        file_count = len(file_list)
        if file_count <= 0:
            logger.debug(f'no file found:{subj}:{event}:{sess_glob}')
            continue
        elif file_count > 1:
            logger.debug(f'too many files:{subj}:{event}:{sess_glob}')
            continue

        # Upload file to redcap
        edat_file = file_list[0]
        logger.debug(f'uploading file:{edat_file}')
        try:
            result = upload_file(
                project,
                record_id,
                raw_field,
                edat_file,
                event_id=event)

            logger.info(f'uploaded:{subj}:{event}:{edat_file}')
        except (ValueError) as err:
            logger.error(f'error uploading:{edat_file}:{err}')

        if not result:
            logger.error(f'upload failed:{subj}:{event}')
            continue

        logger.info(f'uploaded:{subj}:{event}')
        results.append({
            'result': 'COMPLETE',
            'description': 'edat_limbo2redcap',
            'category': 'edat_limbo2redcap',
            'subject': subj,
            'session': '',
            'scan': '',
            'event': event,
            'field': raw_field})

    return results
