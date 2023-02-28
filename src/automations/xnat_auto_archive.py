import logging

from ..utils_xnat import copy_session, session_label_list

# Copy session from project named for PI to primary project.
# Session ID generated from event 2 session mapping in project settings
# This method allows the PI project on XNAT to be set to auto-archive and
# skips the prearchive to avoid timeout problems moving large sessions.


def process_project(
    xnat,
    scan_table,
    src_project,
    dst_project,
):
    """Copy from src to dst as needed."""
    results = []
    dst_sess_list = session_label_list(xnat, dst_project)
    src_sess_list = session_label_list(xnat, src_project)

    # Process each record
    for r in scan_table:
        src_subj = r['src_subject']
        src_sess = r['src_session']
        dst_subj = r['dst_subject']
        dst_sess = r['dst_session']

        # Remove leading and trailing whitespace that keeps showing up
        src_sess = src_sess.strip()

        # Check if session already exists in destination project
        if dst_sess in dst_sess_list:
            # Note that we don't check the other values in redcap
            logging.debug(f'session exists on XNAT:{dst_sess}')
            continue

        # Check that session does exist in source project
        if src_sess not in src_sess_list:
            logging.info(f'session not on XNAT:{src_sess}:{dst_subj}')
            continue

        # TODO: check that xnat date matches redcap date
        # TODO: check last_modified and wait until it's been 1 hour
        logging.info(f'copying:{src_subj}/{src_sess}:{dst_subj}/{dst_sess}')
        src_obj = xnat.select_session(src_project, src_subj, src_sess)
        dst_obj = xnat.select_session(dst_project, dst_subj, dst_sess)
        copy_session(src_obj, dst_obj)
        results.append({
            'result': 'COMPLETE',
            'description': f'{src_sess}',
            'subject': dst_subj,
            'session': dst_sess})

    return results
