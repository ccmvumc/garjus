"""Garjus Issues Management."""
import logging
import importlib


logger = logging.getLogger('garjus.issues')


def update(garjus, projects=None):
    """Update issues."""

    # First find unmatched sessions across projects.
    # these are sessions that are in source project
    # but not found in destination projects
    unmatched = _unmatched(garjus)

    # Update each project
    for p in (projects or garjus.projects()):
        if p in projects:
            logger.debug(f'updating issues:{p}')
            update_project(garjus, p, unmatched[p])


def _unmatched(garjus):
    src2dst = {}
    src2ignore = {}
    results = []
    unmatched = {}

    # Build the list of dst projects (and ignore list) for each source project
    for dst_project in garjus.projects():
        unmatched[dst_project] = []

        for p in garjus.scanning_protocols(dst_project):
            src_project = p['scanning_srcproject']

            if not src_project:
                logger.debug(f'no scanning_srcproject')
                continue

            # Get sessions to ignore
            ignore_sessions = p['scanning_ignore'].split(',')
            ignore_sessions = [x.strip() for x in ignore_sessions]

            # Append dst project to list
            if src_project not in src2dst:
                # Make a new list
                src2dst[src_project] = [dst_project]
                src2ignore[src_project] = ignore_sessions

            elif dst_project not in src2dst[src_project]:
                # Add to existing lists
                src2dst[src_project].append(dst_project)
                src2ignore[src_project].extend(ignore_sessions)

    # Find unmatched in each source project
    for src_project, dst_projects in src2dst.items():
        logger.debug(f'finding unmatched sessions:{src_project}')
        src_labels = garjus.session_labels(src_project)
        src_ignore = src2ignore[src_project]

        # Build the list of src IDs for sessions in the destination projects
        srcid_list = []
        for dst_project in dst_projects:
            # These are the original session labels before being renamed
            srcid_list += garjus.session_source_labels(dst_project)

        # Apply ignore list
        src_labels = [x for x in src_labels if x not in src_ignore]

        # Get unmatched, not in list of sources in destination
        src_unmatched = [x for x in src_labels if x not in srcid_list]

        # Create an issue for each dst project, we don't know which is the dst
        for sess in src_unmatched:
            logger.debug(f'unmatched session:{sess}')
            for dst_project in dst_projects:
                unmatched[dst_project].append(sess)

    return unmatched


def update_project(garjus, project, unmatched):
    """Update project issues."""
    results = []

    # Load the project primary redcap
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logger.debug('primary redcap not found, cannot update issues')
        return

    # Confirm project exists on XNAT
    if not garjus.project_exists(project):
        msg = f'destination project does not exist:{project}'
        logger.debug(msg)
        return

    # Audit edats
    for p in garjus.edat_protocols(project):
        # Load event list from comma-delimited value
        events = p['edat_events']
        events = [x.strip() for x in events.split(',')]

        # Run the audit
        results += _audit_edat(
            project_redcap,
            events,
            p['edat_rawfield'],
            p['edat_convfield'],
            p['edat_readyfield'])

    # Scanning protocols
    for p in garjus.scanning_protocols(project):
        events = p['scanning_events']
        date_field = p['scanning_datefield']
        sess_field = p['scanning_srcsessfield']
        sess_suffix = p['scanning_xnatsuffix']
        src_project = p['scanning_srcproject']

        if not date_field:
            logger.debug(f'missing date field')
            continue

        if not sess_field:
            logger.debug(f'missing sess field')
            continue

        src_labels = garjus.session_labels(src_project)
        dst_labels = garjus.session_labels(project)

        # Get events list
        events = None
        if p.get('scanning_events', False):
            events = [x.strip() for x in p['scanning_events'].split(',')]

        # check that projects exist on XNAT
        if not garjus.source_project_exists(src_project):
            msg = f'source project does not exist in XNAT:{src_project}'
            logger.error(msg)
            garjus.add_issue(msg, project, category='ERROR')
            continue

        # Check for alternate redcap
        if p['scanning_altprimary'] != '':
            _rc = garjus.alternate(p['scanning_altprimary'])
        else:
            _rc = project_redcap

        # Make the scan table linking scan ID to subject/session
        scan_table = _make_scan_table(
            _rc,
            events,
            date_field,
            sess_field,
            sess_suffix)

        results += _audit_scanning(scan_table, src_labels, dst_labels)

    # Issues for unmatched
    for sess in unmatched:
        results.append({
            'project': project,
            'category': 'UNMATCHED_SESSION',
            'session': sess,
            'description': sess})

    _add_issues(garjus, results, project)


def _matching_issues(issue1, issue2):
    # Matching means both issues are of the same category
    # and on the same Project/Subject
    # and as applicable, the same XNAT Session/Scan
    # and as applicable the same REDCap Event/Field
    keys = [
        'PROJECT', 'CATEGORY', 'SUBJECT', 'SESSION', 'SCAN', 'EVENT', 'FIELD']

    for k in keys:
        if (k.lower() in issue1) and (issue1[k.lower()] != issue2[k]):
            return False

    # Everything matched
    return True


def _audit_edat(project, events, rawfield, convfield, readyfield):
    try:
        audit_edat = importlib.import_module(f'garjus.issues.audit_edat')
    except ModuleNotFoundError as err:
        logger.error(f'error loading:{err}')
        return

    return audit_edat.audit(project, events, rawfield, convfield, readyfield)


def _audit_scanning(scan_table, src_project, project):
    try:
        audit_imaging = importlib.import_module(f'garjus.issues.audit_imaging')
    except ModuleNotFoundError as err:
        logger.error(f'error loading:{err}')
        return

    return audit_imaging.audit(scan_table, src_project, project)


def _add_issues(garjus, records, project):
    new_issues = []
    old_issues = []
    has_errors = False
  
    # First check existing issues,
    # import new issues and update existing,
    # complete any no longer found

    # Check for errors
    for r in records:
        if r['category'] == 'ERROR':
            has_errors = True
            break

    # Set project
    for r in records:
        r['project'] = project

    # Compare to current issues
    cur_issues = garjus.issues(project)
    new_issues = _find_new(cur_issues, records)

    # Upload new records
    if new_issues:
        logger.debug(f'uploading {len(new_issues)} new issues')
        garjus.add_issues(new_issues)
    else:
        logger.debug('no new issues to upload')

    if has_errors:
        logger.debug(f'errors during audit, not closing old issues')
        return

    # Close fixed issues
    fixed_issues = _find_fixed(cur_issues, records)
    if fixed_issues:
        logger.debug(f'setting {len(fixed_issues)} resolved issues to complete')
        garjus.close_issues(fixed_issues)
    else:
        logger.debug('no resolved issues to complete')


def _find_new(issues, records):
    results = []

    for rec in records:
        isnew = True

        # Try to find a matching record
        for cur in issues.to_dict('records'):
            cur_id = cur['ID']
            cur_proj = cur['PROJECT']
            if _matching_issues(rec, cur):
                isnew = False
                logger.debug(f'matches existing issue:{cur_proj}:{cur_id}')
                break

        if isnew:
            results.append(rec)
    return results


def _find_fixed(issues, records):
    """Return issues that are not in current search, i.e. resolved"""
    results = []
    # Find old issues
    logger.debug('checking for resolved issues')
    for cur in issues.to_dict('records'):
        isold = True
        cur_id = cur['ID']
        cur_proj = cur['PROJECT']

        # Try to find a matching record
        for rec in records:
            if _matching_issues(rec, cur):
                isold = False
                logger.debug(f'matches existing issue:{cur_proj}:{cur_id}')
                break

        if isold:
            # Append to list as closed with current time
            logger.debug(f'found resolved issue:{cur_proj}:{cur_id}')
            results.append({'project': cur_proj, 'id': cur_id})

    return results


def _make_scan_table(
    project,
    events,
    date_field,
    sess_field,
    scan_suffix):
    """Make the scan table, linking source to destination subject/session."""
    data = []
    id2subj = {}

    # Shortcut
    def_field = project.def_field

    # Handle secondary ID
    sec_field = project.export_project_info()['secondary_unique_field']
    if sec_field:
        rec = project.export_records(fields=[def_field, sec_field])
        id2subj = {x[def_field]: x[sec_field] for x in rec if x[sec_field]}

    # Get mri records from redcap
    fields = [date_field, sess_field, def_field]
    try:
        rec = project.export_records(fields=fields, events=events)
    except Exception as err:
        logger.error(f'cannot make scan table:{err}:{fields}')
        return []

    # Only if date is entered
    rec = [x for x in rec if x[date_field]]

    # Only if entered
    rec = [x for x in rec if x[sess_field]]

    # Set the subject and session
    for r in rec:
        d = {}
        d['scandate'] = r[date_field]
        d['src_session'] = r[sess_field]
        d['src_subject'] = d['src_session']
        d['dst_subject'] = id2subj.get(r[def_field], r[def_field])
        d['dst_session'] = d['dst_subject'] + scan_suffix
        if 'redcap_event_name' in r:
            d['event'] = r['redcap_event_name']
        else:
            d['event'] = ''

        data.append(d)

    return data
