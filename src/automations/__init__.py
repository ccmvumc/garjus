"""

Garjus automations.

Automation names corresond to folder name.

"""
import logging
import importlib


def update(garjus, projects, autos_include=None, autos_exclude=None):
    """Update project progress."""
    for p in projects:
        logging.info(f'updating automations:{p}')
        update_project(garjus, p, autos_include, autos_exclude)


def update_project(garjus, project, autos_include=None, autos_exclude=None):
    """Update automations for project."""
    results = []

    # Get filtered list for this project
    scan_autos = garjus.scan_automations(project)

    if autos_include:
        # Apply include filter
        scan_autos = [x for x in scan_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        scan_autos = [x for x in scan_autos if x not in autos_exclude]

    _run_scan_automations(scan_autos, garjus, project)

    etl_autos = garjus.etl_automations(project)

    if autos_include:
        # Apply include filter
        etl_autos = [x for x in etl_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        etl_autos = [x for x in etl_autos if x not in autos_exclude]

    for a in etl_autos:
        logging.info(f'{project}:running automation:{a}')
        _run_etl_automation(a, garjus, project)

    return results


def _parse_scanmap(scanmap):
    """Parse scan map stored as string into map."""
    # Parse multiline string of delimited key value pairs into dictionary
    scanmap = dict(x.strip().split(':') for x in scanmap.split('\n'))

    # Remove extra whitespace from keys and values
    scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}

    return scanmap


def _run_etl_automation(automation, garjus, project):
    """Load the project primary redcap."""
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('not found')
        return

    # load the automation
    try:
        m = importlib.import_module(f'src.automations.{automation}')
    except ModuleNotFoundError as err:
        logging.error(f'error loading module:{automation}:{err}')
        return

    # Run it
    try:
        results = m.process_project(project_redcap)
    except Exception as err:
        logging.error(f'{project}:{automation}:failed to run:{err}')
        # garjus.add_issue()
        return

    # Upload results to garjus
    for r in results:
        r.update({'project': project, 'category': automation})
        r.update({'description': r.get('description', automation)})
        garjus.add_activity(**r)


def _run_scan_automations(automations, garjus, project):
    results = []
    proj_scanmap = garjus.project_setting(project, 'scanmap')
    sess_replace = garjus.project_setting(project, 'relabelreplace')
    scan_data = garjus.scanning_protocols(project)
    site_data = garjus.sites(project)
    protocols = garjus.scanning_protocols(project)

    # Build the session relabling
    sess_relabel = _session_relabels(scan_data, site_data)

    # Parse scan map
    if proj_scanmap:
        proj_scanmap = _parse_scanmap(proj_scanmap)

    # Load the project primary redcap
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('primary redcap not found, cannot run automations')
        return

    # load the automations
    try:
        xnat_auto_archive = importlib.import_module(f'src.automations.xnat_auto_archive')
        xnat_relabel_sessions = importlib.import_module(f'src.automations.xnat_relabel_sessions')
        xnat_relabel_scans = importlib.import_module(f'src.automations.xnat_relabel_scans')
    except ModuleNotFoundError as err:
        logging.error(f'error loading scan automations:{err}')
        return

    # Get xnat connection
    xnat = garjus.xnat()

    # Apply autos to each scanning protocol
    for p in protocols:
        date_field = p['scanning_datefield']
        sess_field = p['scanning_srcsessfield']
        sess_suffix = p['scanning_xnatsuffix']
        src_project = p['scanning_srcproject']

        # Get events list
        events = None
        if p.get('scanning_events', False):
            events = [x.strip() for x in p['scanning_events'].split(',')]

        # Make the scan table that links what's entered at the scanner with
        # what we want to label the scans
        scan_table = _make_scan_table(
            project_redcap,
            events,
            date_field,
            sess_field,
            sess_suffix)

        # Run
        if 'xnat_auto_archive' in automations:
            results += xnat_auto_archive.process_project(
                garjus, scan_table, src_project, project)

    # Apply relabeling
    if 'xnat_relabel_sessions' in automations:
        results += xnat_relabel_sessions.process_project(
            xnat, project, sess_relabel, sess_replace)
    if 'xnat_relabel_scans' in automations:
        results += xnat_relabel_scans.process_project(
            xnat, project, proj_scanmap)

    # Upload results to garjus
    for r in results:
        r['project'] = project
        garjus.add_activity(**r)


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
        logging.error(err)
        return []

    # Only if date is entered
    rec = [x for x in rec if x[date_field]]

    # Only if entered
    rec = [x for x in rec if x[sess_field]]

    # Set the subject and session
    for r in rec:
        d = {}
        d['src_session'] = r[sess_field]
        d['src_subject'] = d['src_session']
        d['dst_subject'] = id2subj.get(r[def_field], '')
        d['dst_session'] = d['dst_subject'] + scan_suffix
        data.append(d)

    return data


def _session_relabels(scan_data, site_data):
    """Build session relabels."""
    relabels = []

    # Build the session relabeling from scan_autos and sites
    for rec in scan_data:
        relabels.append((
            'session_label',
            '*' + rec['scanning_xnatsuffix'],
            'session_type',
            rec['scanning_xnattype']))

    for rec in site_data:
        relabels.append((
            'session_label',
            rec['site_sessmatch'],
            'site',
            rec['site_shortname']))

    return relabels
