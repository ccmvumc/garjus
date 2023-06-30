"""Garjus audit imaging."""
import logging


logger = logging.getLogger('garjus.issues.audit_imaging')


def audit(scan_table, src_labels, dst_labels):
    """find issues with scan archiving."""
    results = []
 
    # Process each record
    for r in scan_table:
        result = _audit_record(r, src_labels, dst_labels)
        if result:
            results.append(result)

    return results


def _audit_record(record, src_labels, dst_labels):
    src_subj = record['src_subject']
    src_sess = record['src_session']
    dst_subj = record['dst_subject']
    dst_sess = record['dst_session']
    scandate = record['scandate']
    event = record['event']
    result = {}

    if dst_subj.startswith('v'):
        dst_subj = dst_subj.replace('v', 'V')

    if dst_sess.startswith('v'):
        # Hard code this to avoid duplicates
        dst_sess = dst_sess.replace('v', 'V')

    # Remove PI prefix if present
    if '_' in src_sess:
        logger.debug(f'{src_sess}:removing PI prefix')
        src_sess = src_sess.split('_')[1]

    # Find problems
    if dst_sess in dst_labels:
        # Ignore record if destination session already exists
        logger.debug(f'{dst_sess}:already archived')
        return None
    elif not src_sess:
        msg = f'{dst_subj}:source session not set'
        logger.debug(msg)
        result.update({'category': 'MISSING_VALUE', 'description': msg})
    elif not dst_sess:
        msg = f'{dst_subj}:destination session not set'
        logger.debug(msg)
        result.update({'category': 'MISSING_VALUE', 'description': msg})
    elif not scandate:
        # We are logging but ignoring this scenario which means we would not 
        # catch the case where user forgets to enter the session date.
        msg = f'{dst_subj}:{dst_sess}:date not set'
        logger.debug(msg)
        return None
    elif src_sess not in src_labels:
        # Check that session does actually exist in source project
        msg = f'{src_sess}:not in source project'
        logger.debug(msg)
        result.update({'category': 'MISSING_SESSION', 'description': msg})
    elif dst_sess not in dst_labels:
        # Add issue that auto archive needs to run
        msg = f'{src_sess}:auto archive not working'
        logger.debug(msg)
        result.update({'category': 'NEEDS_AUTO', 'description': msg})

    # Add other details
    result.update({
        'subject': dst_subj,
        'session': dst_sess,
        'event': event,
        'date': scandate
        })

    return result
