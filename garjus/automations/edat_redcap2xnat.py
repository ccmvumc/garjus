import glob
import re
import logging
import tempfile

from ..utils_xnat import upload_file
from ..utils_redcap import download_file


logger = logging.getLogger('garjus.automations.edat_redcap2xnat')


def process_project(
    xnat,
    project,
    events,
    tab_field,
    event2sess,
    scans,
    xnat_scan_type,
    xnat_scan_resource,
):
    results = []
    def_field = project.def_field
    fields = [def_field, tab_field]
    id2subj = {}

    # Get the subject id map
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
    else:
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
            logger.debug(f'record without subj num:{record_id}:{err}')
            continue

        logger.debug(f'{record_id}:{event}:{subj}')

        # Check for converted file
        if not r[tab_field]:
            logger.debug(f'{record_id}:{subj}:{event}:not yet converted')
            continue

        # Check if it needs uploading
        try:
            session = subj + event2sess[event]
        except KeyError:
            logger.error(f'{subj}:{event}:unknown event')
            continue

        # Filter scans for this session
        sess_scans = [x for x in scans if x['SESSION'] == session]
        sess_scans = [x for x in sess_scans if x['SCANTYPE'] == xnat_scan_type]

        # Filter scans to exclude unusable
        sess_scans = [x for x in sess_scans if x['QUALITY'] != 'unusable']

        if len(sess_scans) <= 0:
            logger.debug(f'scan not found:{subj}:{event}:{session}')
            continue

        if len(sess_scans) > 1:
            logger.debug(f'multiple scans, uhhh:{subj}:{event}:{session}')
            print(sess_scans)
            continue

        # Only one scan found so use it
        scan = sess_scans[0]
        scan_label = scan['SCANID']

        # Check for existing resource
        if xnat_scan_resource in scan['RESOURCES']:
            logger.debug(f'{session}:{scan_label}:{xnat_scan_resource}:exists')
            continue

        # Copy the file
        with tempfile.TemporaryDirectory() as tmpdir:
            # Download the tab file from redcap to tmp
            tab_file = f'{tmpdir}/{r[tab_field]}'
            result = download_file(
                project,
                record_id,
                tab_field,
                tab_file,
                event_id=event)

            if not result:
                logger.error(f'{subj}:{event}:download failed')
                continue

            resource = xnat.select_scan_resource(
                scan['PROJECT'],
                scan['SUBJECT'],
                scan['SESSION'],
                scan['SCANID'],
                xnat_scan_resource)
    
            # Upload file to xnat resource
            result = upload_file(tab_file, resource)

            if not result:
                logger.error(f'{subj}:{session}:{scan_label}:upload fail')
                continue

        logger.info(f'{subj}:{event}:{session}:{scan_label}:uploaded')
        results.append({
            'result': 'COMPLETE',
            'description': 'edat_redcap2xnat',
            'category': 'edat_redcap2xnat',
            'subject': subj,
            'session': session,
            'scan': scan['SCANID'],
            'event': event,
            'field': tab_field})

    return results
