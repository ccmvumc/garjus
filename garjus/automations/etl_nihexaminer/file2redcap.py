import glob
import re
import logging

from ...utils_redcap import upload_file, field2events


logger = logging.getLogger('garjus.automations.etl_nihexaminer.files2redcap')


# Finds NIH examiner summary files and uploads to REDCap.


def process_project(
    project,
    limbo_dir,
    event2sess
):
    results = []
    def_field = project.def_field
    id2subj = {}
    flank_field = 'flanker_file'
    nback_field = 'nback_upload'
    shift_field = 'set_shifting_file'
    cpt_field = 'cpt_upload'
    done_field = 'flanker_score'

    if 'flanker_summfile' in project.field_names:
        # Alternate file field names used in some old REDCap projects
        flank_field = 'flanker_summfile'
        nback_field = 'nback_summfile'
        shift_field = 'set_shifting_summfile'
        cpt_field = 'cpt_summfile'

    # Set the names of the fields we want to query
    fields = [def_field, flank_field, nback_field, shift_field, cpt_field, done_field]

    # Determine events
    events = field2events(project, cpt_field)

    # Handle secondary ID
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

        # Find files for this record
        logger.debug(f'looking for files:{subj}:{event}')

        # Get just the numeric portion of the subject number
        subj_num = re.sub(r'[^0-9]', '', subj)

        # Find files for this subject
        for cur_field, cur_name in zip(
            [flank_field, nback_field, shift_field, cpt_field], 
            ['Flanker', 'SetShifting', 'CPT', 'NBack']):

            # Check existing
            if r[cur_field]:
                logger.debug(f'already uploaded:{subj}:{event}:{cur_field}')
                continue

            cur_glob = f'{limbo_dir}/{subj_num}/{cur_name}_Summary_{subj_num}_{sess_num}_*.csv'
            file_list = sorted(glob.glob(cur_glob))
            file_count = len(file_list)
            if file_count <= 0:
                logger.debug(f'no file found:{subj}:{event}:{cur_glob}')
                continue
            elif file_count > 1:
                logger.debug(f'too many files:{subj}:{event}:{cur_glob}')
                continue

            #Upload file to redcap
            cur_file = file_list[0]
            logger.debug(f'uploading file:{cur_file}')

            try:
               result = upload_file(
                   project,
                   record_id,
                   cur_field,
                   cur_file,
                   event_id=event)

               logger.debug(f'uploaded:{subj}:{event}:{cur_file}')
            except (ValueError) as err:
               logger.error(f'error uploading:{cur_file}:{err}')

            if not result:
               logger.error(f'upload failed:{subj}:{event}')
               continue

            logger.debug(f'uploaded:{subj}:{event}')
            results.append({
               'result': 'COMPLETE',
               'description': 'etl_nihexaminer.file2redcap',
               'category': 'etl_nihexaminer.file2redcap',
               'subject': subj,
               'session': '',
               'scan': '',
               'event': event,
               'field': cur_field})

    return results
