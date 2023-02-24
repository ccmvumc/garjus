from dax import XnatUtils

from xnat_copy_session import copy_session
from logs import logger

import utils_xnat


# DONE: don't require redcap to store the session number, we can just
# automatically generate it based on event and a setting for the project

# Data Sources:
# REDCap
# XNAT

# this method replaces the old dax module "Module_Auto_Archive.py".
# This method allows the PI project on XNAT to be set to auto-archive which is
# how most projects are being set up on vuiis xnat. This skips the prearchive
# which means we don't have to click to do anything and it avoids the problems
# with the module where moving the whole session would time out.
# Also, we can now let all scans go to the PI project and only keep specific
# scans in the session on the real project. meaning we can just delete any bad,
# unusable, or extraneous scans. If space is an issue, we could choose to even
# delete the DICOM after NIFIT is created.

# Purpose: Copy session from project named for PI to real project

# TODO: auto archive allow use of local OneDrive folder as Inbox, then
# run import_dicom.py code to import the zip, file should be a few hours od
# before we try to use it




def run(garjus, project):



    fields = [def_field, date_field, src_sess_field, dst_sess_field]
    id2subj = {}



    logger.info(f'loading session info from XNAT:{dst_project_name}:{src_project_name}')
    dst_sess_list = utils_xnat.session_label_list(xnat, dst_project_name)
    src_sess_list = utils_xnat.session_label_list(xnat, src_project_name)



        #if use_secondary:
        #    # Handle secondary ID
        #    sec_field = project.export_project_info()['secondary_unique_field']
        #    if not sec_field:
        #        logger.error('secondary enabled, but no secondary field found')
        #        return
        #
        #    rec = project.export_records(fields=[def_field, sec_field])
        #    id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

        # Get mri records from redcap
        rec = project.export_records(fields=fields, events=events)

        # Process each record
        for r in rec:
            record_id = r[def_field]
            if 'redcap_event_name' in r:
                event_id = r['redcap_event_name']
            else:
                event_id = 'None'

            # Get the source labels
            if '_' in r[src_sess_field]:
                # Ignore PI prefix if present
                src_sess = r[src_sess_field].split('_')[1]
            else:
                src_sess = r[src_sess_field]

            # Remove leading and trailing whitespace that keeps showing up
            src_sess = src_sess.strip()

            src_subj = src_sess
            src_date = r[date_field]

            # Get the destination labels
            if use_secondary:
                try:
                    dst_subj = id2subj[record_id]
                except KeyError as err:
                    logger.info(f'record without subject number:{err}')
                    continue
            else:
                dst_subj = record_id

            if event2sess is not None:
                # Check if event2sess is not none, then get destination session
                # label by mapping event 2 session and then concatenate with
                # subject
                try:
                    suffix = event2sess[event_id]
                    dst_sess = dst_subj + suffix
                except KeyError:
                    logger.error(f'{record_id}:{event_id}:failed to map event to session suffix')
                    continue
            elif dst_sess_field is not None:
                dst_sess = r[dst_sess_field]
            else:
                logger.info(f'{record_id}:{event_id}:failed to get session ID')
                continue

            # Check for missing values
            if not dst_sess:
                logger.info(f'{record_id}:{event_id}:destination not set')
                continue

            # Check if session already exists in destination project
            if dst_sess in dst_sess_list:
                # Note that we don't check the other values in redcap
                logger.debug(f'session exists on XNAT:{dst_sess}')
                continue

            if not src_date:
                logger.debug(f'{record_id}:{event_id}:date not set')
                continue

            if not src_sess:
                logger.debug(f'{record_id}:{event_id}:source session not set')
                continue

            # Check that session does exist in source project
            if src_sess not in src_sess_list:
                logger.info(f'session not on XNAT:{src_sess}')
                continue

            # Check that xnat date matches redcap date

            # TODO: What else should we check on xnat before we start copying?
            # Is there a timestamp we could check, like last_modified and wait
            # until it's been 2 hours? YES! need to wait somehow!
            logger.info('{0}:{1}:{2}:{3}:{4}:{5}'.format(
                'copy', src_subj, src_sess, dst_subj, dst_sess, src_date))
            src_obj = xnat.select_session(src_project_name, src_subj, src_sess)
            dst_obj = xnat.select_session(dst_project_name, dst_subj, dst_sess)
            copy_session(src_obj, dst_obj)

            garjus.set_activity(
                project, {
                    'result': 'COMPLETE',
                    'type': 'xnat_auto_archive',
                    'event': event_id,
                    'subject': dst_subj,
                    'session': dst_sess}
            )

    return results



def process_project(
    project,
    events,
    date_field,
    src_sess_field,
    dst_sess_field,
    src_project_name,
    dst_project_name,
    use_secondary=False,
    event2sess=None
):
    results = []
    def_field = project.def_field
    fields = [def_field, date_field, src_sess_field, dst_sess_field]
    id2subj = {}

    with XnatUtils.InterfaceTemp(xnat_retries=0) as xnat:

        # check that projects exist on XNAT
        if not xnat.select.project(src_project_name).exists():
            logger.error(f'source project not on XNAT:{src_project_name}')
            return

        # check that projects exist on XNAT
        if not xnat.select.project(dst_project_name).exists():
            logger.error(f'destination project not on XNAT:{dst_project_name}')
            return

        logger.info(f'loading session info from XNAT:{dst_project_name}:{src_project_name}')
        dst_sess_list = utils_xnat.session_label_list(xnat, dst_project_name)
        src_sess_list = utils_xnat.session_label_list(xnat, src_project_name)

        if use_secondary:
            # Handle secondary ID
            sec_field = project.export_project_info()['secondary_unique_field']
            if not sec_field:
                logger.error('secondary enabled, but no secondary field found')
                return

            rec = project.export_records(fields=[def_field, sec_field])
            id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}
            # TODO: should we chedck that subject matches session prefix?

        # Get mri records from redcap
        rec = project.export_records(fields=fields, events=events)

        # Process each record
        for r in rec:
            record_id = r[def_field]
            if 'redcap_event_name' in r:
                event_id = r['redcap_event_name']
            else:
                event_id = 'None'

            # Get the source labels
            if '_' in r[src_sess_field]:
                # Ignore PI prefix if present
                src_sess = r[src_sess_field].split('_')[1]
            else:
                src_sess = r[src_sess_field]

            # Remove leading and trailing whitespace that keeps showing up
            src_sess = src_sess.strip()

            src_subj = src_sess
            src_date = r[date_field]

            # Get the destination labels
            if use_secondary:
                try:
                    dst_subj = id2subj[record_id]
                except KeyError as err:
                    logger.info(f'record without subject number:{err}')
                    continue
            else:
                dst_subj = record_id

            if event2sess is not None:
                # Check if event2sess is not none, then get destination session
                # label by mapping event 2 session and then concatenate with
                # subject
                try:
                    suffix = event2sess[event_id]
                    dst_sess = dst_subj + suffix
                except KeyError:
                    logger.error(f'{record_id}:{event_id}:failed to map event to session suffix')
                    continue
            elif dst_sess_field is not None:
                dst_sess = r[dst_sess_field]
            else:
                logger.info(f'{record_id}:{event_id}:failed to get session ID')
                continue

            # Check for missing values
            if not dst_sess:
                logger.info(f'{record_id}:{event_id}:destination not set')
                continue

            # Check if session already exists in destination project
            if dst_sess in dst_sess_list:
                # Note that we don't check the other values in redcap
                logger.debug(f'session exists on XNAT:{dst_sess}')
                continue

            if not src_date:
                logger.debug(f'{record_id}:{event_id}:date not set')
                continue

            if not src_sess:
                logger.debug(f'{record_id}:{event_id}:source session not set')
                continue

            # Check that session does exist in source project
            if src_sess not in src_sess_list:
                logger.info(f'session not on XNAT:{src_sess}')
                continue

            # Check that xnat date matches redcap date

            # TODO: What else should we check on xnat before we start copying?
            # Is there a timestamp we could check, like last_modified and wait
            # until it's been 2 hours? YES! need to wait somehow!
            logger.info('{0}:{1}:{2}:{3}:{4}:{5}'.format(
                'copy', src_subj, src_sess, dst_subj, dst_sess, src_date))
            src_obj = xnat.select_session(src_project_name, src_subj, src_sess)
            dst_obj = xnat.select_session(dst_project_name, dst_subj, dst_sess)
            copy_session(src_obj, dst_obj)
            results.append({
                    'result': 'COMPLETE',
                    'type': 'xnat_auto_archive',
                    'event': event_id,
                    'subject': dst_subj,
                    'session': dst_sess})

    return results
