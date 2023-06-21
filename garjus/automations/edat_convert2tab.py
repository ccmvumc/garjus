import logging
import os
import time

from ..utils_redcap import upload_file, download_file


logger = logging.getLogger('garjus.automations.edat_limbo2redcap')


def process_project(
    project,
    events,
    raw_field,
    tab_field,
    convert_dir,
    wait_time=70,
    wait_count=3
):
    results = []
    def_field = project.def_field
    fields = [def_field, raw_field, tab_field]
    id2subj = {}

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

        try:
            subj = id2subj[record_id]
        except KeyError as err:
            logger.debug(f'record without secondary subject number:{err}')
            continue

        logger.debug(f'{subj}:{event}:{record_id}')

        # Check for existing converted file
        if r[tab_field]:
            logger.debug(f'already converted:{subj}:{event}')
            continue

        # Check for no raw file
        if not r[raw_field]:
            logger.debug(f'no raw edat file:{subj}:{event}')
            continue

        # Check for missing data flag
        if 'MISSING_DATA' in r[raw_field]:
            logger.debug(f'missing:{subj}:{event}')
            continue

        # Download to the convert dir
        basename = f'{subj}_{event}_{raw_field}.edat2'
        raw_file = f'{convert_dir}/{basename}'
        logger.info(f'downloading {subj}:{event}:{raw_file}')

        res = download_file(
            project, record_id, raw_field, raw_file, event_id=event)
        if not res:
            logger.debug(f'dowload failed:{subj}:{event}')
            continue

        # here we wait for it to be converted and then try to find it
        found = False
        tab_file = f'{raw_file}_tab.txt'
        for i in range(wait_count):
            if os.path.exists(tab_file):
                found = True
                break

            logger.info(f'{subj}:{event}:waiting for converted file:{i}')
            time.sleep(wait_time)

        if not found:
            logger.error(f'converted file not found:{subj}:{event}:{tab_file}')
            continue

        # Upload the converted file to redcap
        try:
            result = upload_file(
                project,
                record_id,
                tab_field,
                tab_file,
                event_id=event)
        except (ValueError) as err:
            logger.error(f'upload failed:{subj}:{event}:{tab_file}:{err}')

        if not result:
            logger.error(f'upload failed:{subj}:{event}')
            continue

        logger.debug(f'uploaded:{subj}:{event}:{tab_file}')
        results.append({
            'result': 'COMPLETE',
            'description': 'edat_convert2tab',
            'category': 'edat_convert2tab',
            'subject': subj,
            'event': event,
            'field': tab_field})

    return results
