"""

Garjus automations.

Automation names corresond to folder name.

"""
import logging
import importlib


def update(garjus, projects, autos_include=None, autos_exclude=None):
    """Update project progress."""
    for p in projects:
        update_project(garjus, p, autos_include, autos_exclude)


def update_project(garjus, project, autos_include=None, autos_exclude=None):
    """Update automations for project."""
    results = []
    proj_scanmap = garjus.project_setting(project, 'scanmap')
    sess_replace = garjus.project_setting(project, 'relabelreplace')

    # Load the project primary redcap
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('not found')
        return

    if proj_scanmap:
        proj_scanmap = parse_scanmap(proj_scanmap)

    # Get filtered list for this project
    scan_autos = garjus.scan_automations(project)

    if autos_include:
        # Apply include filter
        scan_autos = [x for x in scan_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        scan_autos = [x for x in scan_autos if x not in autos_exclude]

    #if 'xnat_auto_archive' in scan_autos:
    #    logging.info(f'{project}:xnat_auto_archive:TBD')

    # Run project-wide relabels
    #if 'xnat_relabel_scans' in scan_autos:
    #    logging.info(f'{project}:xnat_relabel_scans:TBD')

    #if 'xnat_relabel_sessions' in scan_autos:
    #    logging.info(f'{project}:xnat_relabel_sessions:TBD')

    #for a in scan_autos:
    #    logging.info(f'{project}:running automation:{a}')

    run_scan_automations(scan_autos, garjus, project)

    etl_autos = garjus.etl_automations(project)

    if autos_include:
        # Apply include filter
        etl_autos = [x for x in etl_autos if x in autos_include]

    if autos_exclude:
        # Apply exclude filter
        etl_autos = [x for x in etl_autos if x not in autos_exclude]

    for a in etl_autos:
        logging.info(f'{project}:running automation:{a}')
        run_etl_automation(a, garjus, project)

    return results


def parse_scanmap(scanmap):
    """Parses scan map stored as string into map"""

    # Parse multiline string of delimited key value pairs into dictionary
    scanmap = dict(x.strip().split(':') for x in scanmap.split('\n'))

    # Remove extra whitespace from keys and values
    scanmap = {k.strip(): v.strip() for k, v in scanmap.items()}

    return scanmap


def run_etl_automation(automation, garjus, project):
    # Load the project primary redcap
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
        #garjus.add_issue()
        return

    # Upload results to garjus
    for r in results:
        r.update({'project': project, 'category': automation})
        r.update({'description': r.get('description', automation)})
        garjus.add_activity(**r)


def run_scan_automations(automations, garjus, project):
    results = []

    # Load the project primary redcap
    project_redcap = garjus.primary(project)
    if not project_redcap:
        logging.info('not found')
        return

    # Get xnat connection
    xnat = garjus.xnat()

    # Each scanning protocol
    protocols = garjus.scanning_protocols(project)

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
        scan_table = make_scan_table(
            project_redcap,
            events,
            date_field,
            sess_field,
            sess_suffix)

        # load the automation
        try:
           m = importlib.import_module(f'src.automations.xnat_auto_archive')
        except ModuleNotFoundError as err:
            logging.error(f'error loading module:xnat_auto_archive:{err}')
            return

        # Run it
        results += m.process_project(xnat, scan_table, src_project, project)
        # results += xnat_session_relabel()
        # results += xnat_scan_relabel()

    # Upload results to garjus
    for r in results:
        garjus.add_activity(**r)


def make_scan_table(
    project,
    events,
    date_field,
    sess_field,
    scan_suffix):
    """Make the scan table, linking source to destination subject/session."""
    data = []

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
